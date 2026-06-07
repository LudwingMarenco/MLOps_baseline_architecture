import io
import os
from collections import Counter

import joblib
import numpy as np
import pandas as pd
from dagster import (
    AssetKey,
    DefaultSensorStatus,
    In,
    JobDefinition,
    OpExecutionContext,
    Out,
    PartitionsDefinition,
    ResourceParam,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    op,
    sensor,
)
from dagster_duckdb import DuckDBResource

from ml_orchestrator.resources import LocalStorageResource
from utils.parameters import (
    get_latest_partition_materialization,
    get_parameters,
)


def normalize_predictions(pred):
    # predictions are always in 2D: (n_samples, n_outputs)
    pred = np.asarray(pred)
    if pred.ndim == 1:
        return pred[:, None]
    return pred


def get_most_frequent(values):
    counts = pd.Series(values).value_counts()
    if counts.iloc[0] > 1:
        return counts.index[0]
    return None


def aggregate_classification(predictions):
    """
    predictions: (n_models, n_samples, n_outputs)
    returns: (n_samples, n_outputs)
    """
    n_models, n_samples, n_outputs = predictions.shape
    result = np.empty((n_samples, n_outputs), dtype=object)

    for i in range(n_samples):
        for j in range(n_outputs):
            result[i, j] = get_most_frequent(predictions[:, i, j])

    return result


def mean_with_tolerance(values, tolerance):
    values = np.asarray(values, dtype=float)
    mean_val = values.mean()

    rel_diff = np.abs(values - mean_val)

    return mean_val if np.max(rel_diff) <= tolerance else np.nan


def aggregate_regression(predictions, tolerance):

    n_models, n_samples, n_outputs = predictions.shape
    result = np.empty((n_samples, n_outputs), dtype=float)

    for i in range(n_samples):
        for j in range(n_outputs):
            result[i, j] = mean_with_tolerance(
                predictions[:, i, j], tolerance=tolerance
            )
    return result


def aggregate_predictions(predictions, task, tolerance):
    """
    predictions: (n_models, n_samples, n_outputs)
    task: 'classification' or 'regression'
    """
    if task == "classification":
        return aggregate_classification(predictions)

    elif task == "regression":
        return aggregate_regression(predictions, tolerance)

    else:
        raise ValueError(f"Unknown task: {task}")


def most_common(values):
    counter = Counter(values)
    value, count = counter.most_common(1)[0]
    return value


def mean_tolerance(values):
    values = np.asarray(values, dtype=float)
    return float(np.mean(values))


def normalize_output_names(target):
    if isinstance(target, str):
        return [target]
    elif isinstance(target, list):
        return target
    else:
        raise ValueError(f"Invalid target type: {type(target)}")


class ModelChunkedServing:
    def __init__(
        self,
        serving_params: str,
        artifact_model_asset: str,
        accuracy_model_asset: str,
        model_partitions: PartitionsDefinition,
        op_name: str,
    ) -> None:
        self.serving_params = serving_params
        self.artifact_model_asset = artifact_model_asset
        self.accuracy_model_asset = accuracy_model_asset
        self.model_partitions = model_partitions
        self.op_name = op_name

    def create_op(self):
        @op(
            name=self.op_name,
            ins={"data": In(pd.DataFrame)},
            tags={"domain": "ML", "pii": "false"},
            out=Out(dict),
        )
        def _op(
            context: OpExecutionContext,
            data: pd.DataFrame,
            model_persistor: ResourceParam[LocalStorageResource],
            duckdb: ResourceParam[DuckDBResource],
        ) -> dict:
            """
            Operation for model serving in chunked data. Predictions are written to parquet.
            """
            parameters = get_parameters(self.serving_params)
            join_config = parameters["serving"]["join_column"]
            prediction_column = parameters["serving"]["prediction_column"]["name"]
            prediction_column_type = parameters["serving"]["prediction_column"]["type"]

            mapping_key = context.get_mapping_key()

            if mapping_key.startswith("chunk_"):
                # Non-partitioned case
                level = None
                chunk_id = mapping_key.replace("chunk_", "")
            elif mapping_key.startswith("level_"):
                # Partitioned case
                try:
                    parts = mapping_key.split("_")
                    # Expected: ["level", "<level>", "chunk", "<id>"]
                    if len(parts) < 4 or parts[2] != "chunk":
                        raise ValueError(f"Invalid mapping_key format: {mapping_key}")
                    level = "_".join(parts[1:-2]) if len(parts) > 4 else parts[1]
                    chunk_id = parts[-1]
                except Exception:
                    raise ValueError(f"Invalid mapping_key format: {mapping_key}")
            else:
                raise ValueError(f"Unknown mapping_key format: {mapping_key}")

            if level:
                try:
                    join_column = join_config["levels"][level]["name"]
                    join_column_type = join_config["levels"][level]["type"]
                except KeyError:
                    raise ValueError(f"Missing join_column config for level: {level}")
            else:
                join_column = join_config["name"]
                join_column_type = join_config["type"]

            client = model_persistor.get_client()
            serving_data = data.loc[:, data.columns != join_column].values
            predictions = []

            asset_key = AssetKey(self.artifact_model_asset)
            asset_key_aux = AssetKey(self.accuracy_model_asset)
            tolerances = []
            tasks = []
            targets = []

            if not hasattr(context, "models_cache"):
                context.models_cache = {}

            models_cache = context.models_cache

            for partition in self.model_partitions.get_partition_keys():
                materialization = get_latest_partition_materialization(
                    context, asset_key, partition
                )
                materialization_aux = get_latest_partition_materialization(
                    context, asset_key_aux, partition
                )

                if materialization is None:
                    context.log.info(
                        f"No accuracy materialization found for {partition}"
                    )
                else:
                    metadata = materialization.asset_materialization.metadata
                    metadata_aux = materialization_aux.asset_materialization.metadata
                    artifact_path = metadata["artifact_path"].value
                    tolerances.append(metadata_aux["model_accuracy"].value)
                    tasks.append(metadata["task"].value)
                    targets.append(metadata["target"].value)

                    if partition not in models_cache:
                        context.log.info(
                            f"Loading model for partition {partition} from registry"
                        )
                        bucket = artifact_path.split("/")[2]
                        model_name = "/".join(artifact_path.split("/")[3:])
                        response = client.get_object(Bucket=bucket, Key=model_name)
                        buffer = io.BytesIO(response["Body"].read())
                        model = joblib.load(buffer)
                        models_cache[partition] = model
                    else:
                        context.log.info(f"Using cached model {partition}")
                        model = models_cache[partition]

                    pred = model.predict(serving_data)
                    predictions.append(normalize_predictions(pred))

            if not tasks:
                raise RuntimeError("No valid model metadata found for aggregation")

            task = most_common(tasks)
            tolerance = mean_tolerance(tolerances)
            target = targets[0]
            output_names = normalize_output_names(target)

            predictions = np.stack(predictions, axis=0)
            final_predictions = aggregate_predictions(predictions, task, tolerance)

            print(final_predictions)
            if final_predictions.shape[1] == 1:
                final_predictions = final_predictions[:, 0]

            result = pd.DataFrame({join_column: data[join_column].values})

            if len(output_names) == 1:
                result[prediction_column] = final_predictions
                columns_metadata = [prediction_column]
            else:
                if len(output_names) != final_predictions.shape[1]:
                    raise ValueError(
                        f"Number of output names ({len(output_names)}) "
                        f"does not match model outputs ({final_predictions.shape[1]})"
                    )
                columns_metadata = []
                for name, values in zip(output_names, final_predictions.T):
                    column_name = f"{prediction_column}_{name}"
                    result[column_name] = values
                    columns_metadata.append(column_name)

            if level:
                context.log.info(
                    f"Prediction completed for level={level}, chunk={chunk_id} with {len(result)} rows."
                )
            else:
                context.log.info(
                    f"Prediction completed for chunk={chunk_id} with {len(result)} rows."
                )

            should_clean = str(chunk_id) == "00000"
            base_table = parameters["serving"]["table_name"]

            if level:
                table_name = f"{base_table}_{level}".upper()
            else:
                table_name = base_table.upper()

            with snowflake_client.get_connection() as conn:
                table_name = table_name
                database = snowflake_client.database
                schema = snowflake_client.schema_
                if should_clean:
                    with conn.cursor() as cursor:
                        prediction_columns_ddl = ",\n".join(
                            f"{col} {prediction_column_type}"
                            for col in columns_metadata
                        )
                        create_query = f"""
                                        CREATE OR REPLACE TABLE {schema}.{table_name} (
                                        {join_column} {join_column_type},
                                        {prediction_columns_ddl});
                                        """
                        context.log.info(
                            f"Creating or replacing table {table_name} at {schema} schema."
                        )
                        cursor.execute(create_query)

                success, number_chunks, rows_inserted, output = write_pandas(
                    conn,
                    result,
                    table_name=table_name,
                    database=database,
                    schema=schema,
                    auto_create_table=False,
                    overwrite=False,
                    quote_identifiers=False,
                )
            if success:
                context.log.info(
                    f"Writing {rows_inserted} records to {table_name}, number chunks {number_chunks}"
                )

            return {
                "table_name": table_name,
                "prediction_column": columns_metadata,
                "join_column": join_column,
            }

        return _op


class ModelChunkedServing:
    def __init__(
        self,
        serving_params: str,
        artifact_model_asset: str,
        accuracy_model_asset: str,
        model_partitions: PartitionsDefinition,
        op_name: str,
    ) -> None:
        self.serving_params = serving_params
        self.artifact_model_asset = artifact_model_asset
        self.accuracy_model_asset = accuracy_model_asset
        self.model_partitions = model_partitions
        self.op_name = op_name

    def create_op(self):
        @op(
            name=self.op_name,
            ins={"data": In(pd.DataFrame)},
            tags={"domain": "ML", "pii": "false"},
            out=Out(dict),
        )
        def _op(
            context: OpExecutionContext,
            data: pd.DataFrame,
            model_persistor: ResourceParam[LocalStorageResource],
        ) -> dict:
            """
            Operation for model serving in chunked data. Predictions are written to parquet.
            """
            parameters = get_parameters(self.serving_params)
            join_config = parameters["serving"]["join_column"]
            prediction_column = parameters["serving"]["prediction_column"]["name"]

            mapping_key = context.get_mapping_key()

            if mapping_key.startswith("chunk_"):
                level = None
                chunk_id = mapping_key.replace("chunk_", "")
            elif mapping_key.startswith("level_"):
                try:
                    parts = mapping_key.split("_")
                    if len(parts) < 4 or parts[2] != "chunk":
                        raise ValueError(f"Invalid mapping_key format: {mapping_key}")
                    level = "_".join(parts[1:-2]) if len(parts) > 4 else parts[1]
                    chunk_id = parts[-1]
                except Exception:
                    raise ValueError(f"Invalid mapping_key format: {mapping_key}")
            else:
                raise ValueError(f"Unknown mapping_key format: {mapping_key}")

            if level:
                try:
                    join_column = join_config["levels"][level]["name"]
                except KeyError:
                    raise ValueError(f"Missing join_column config for level: {level}")
            else:
                join_column = join_config["name"]

            serving_data = data.loc[:, data.columns != join_column].values
            predictions = []
            tolerances = []
            tasks = []
            targets = []

            asset_key = AssetKey(self.artifact_model_asset)
            asset_key_aux = AssetKey(self.accuracy_model_asset)

            if not hasattr(context, "models_cache"):
                context.models_cache = {}

            models_cache = context.models_cache

            for partition in self.model_partitions.get_partition_keys():
                materialization = get_latest_partition_materialization(
                    context, asset_key, partition
                )
                materialization_aux = get_latest_partition_materialization(
                    context, asset_key_aux, partition
                )

                if materialization is None:
                    context.log.info(f"No materialization found for {partition}")
                else:
                    metadata = materialization.asset_materialization.metadata
                    metadata_aux = materialization_aux.asset_materialization.metadata
                    artifact_path = metadata["artifact_path"].value
                    tolerances.append(metadata_aux["model_accuracy"].value)
                    tasks.append(metadata["task"].value)
                    targets.append(metadata["target"].value)

                    if partition not in models_cache:
                        context.log.info(
                            f"Loading model for partition {partition} from local registry"
                        )
                        model = model_persistor.load(artifact_path)
                        models_cache[partition] = model
                    else:
                        context.log.info(f"Using cached model {partition}")
                        model = models_cache[partition]

                    pred = model.predict(serving_data)
                    predictions.append(normalize_predictions(pred))

            if not tasks:
                raise RuntimeError("No valid model metadata found for aggregation")

            task = most_common(tasks)
            tolerance = mean_tolerance(tolerances)
            target = targets[0]
            output_names = normalize_output_names(target)

            predictions = np.stack(predictions, axis=0)
            final_predictions = aggregate_predictions(predictions, task, tolerance)

            if final_predictions.shape[1] == 1:
                final_predictions = final_predictions[:, 0]

            result = pd.DataFrame({join_column: data[join_column].values})

            if len(output_names) == 1:
                result[prediction_column] = final_predictions
                columns_metadata = [prediction_column]
            else:
                if len(output_names) != final_predictions.shape[1]:
                    raise ValueError(
                        f"Number of output names ({len(output_names)}) "
                        f"does not match model outputs ({final_predictions.shape[1]})"
                    )
                columns_metadata = []
                for name, values in zip(output_names, final_predictions.T):
                    column_name = f"{prediction_column}_{name}"
                    result[column_name] = values
                    columns_metadata.append(column_name)

            base_output = parameters["serving"]["output_path"]
            table_name = parameters["serving"]["table_name"].lower()
            output_dir = os.path.join(base_output, level) if level else base_output
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(
                output_dir, f"{table_name}_chunk_{chunk_id}.parquet"
            )
            result.to_parquet(output_file, index=False)

            if level:
                context.log.info(
                    f"Prediction completed for level={level}, chunk={chunk_id} "
                    f"with {len(result)} rows. Written to {output_file}"
                )
            else:
                context.log.info(
                    f"Prediction completed for chunk={chunk_id} with {len(result)} rows. "
                    f"Written to {output_file}"
                )

            return {
                "output_file": output_file,
                "prediction_column": columns_metadata,
                "join_column": join_column,
            }

        return _op


def conditional_serving(
    job: JobDefinition,
    artifact_model_asset: str,
    model_partitions: PartitionsDefinition,
    sensor_name: str,
    job_partitions: PartitionsDefinition | None = None,
):
    @sensor(
        name=sensor_name,
        job=job,
        minimum_interval_seconds=600,
        default_status=DefaultSensorStatus.STOPPED,
        description="Trigger serving job once models for most recent version are registered.",
    )
    def _sensor(context: SensorEvaluationContext):

        trigger = []
        versions = []

        asset_key = AssetKey(artifact_model_asset)

        for partition in model_partitions.get_partition_keys():

            materialization = get_latest_partition_materialization(
                context, asset_key, partition
            )
            persisted = False
            version = None

            if materialization is not None:
                metadata = materialization.asset_materialization.metadata
                persisted_val = metadata.get("persisted")
                persisted = persisted_val.value if persisted_val is not None else False
                version_val = metadata.get("model_version")
                version = version_val.value if version_val is not None else None

            trigger.append(persisted)

            if persisted and version is not None:
                versions.append(version)

        if not trigger or not all(trigger):
            context.log.info("Not all partitions are persisted.")
            yield SkipReason("Not all partitions are persisted.")
            return

        if not versions:
            context.log.info("No valid model versions found.")
            yield SkipReason("No valid model versions found.")
            return

        if len(set(versions)) != 1:
            context.log.info(f"Inconsistent model versions detected: {versions}")
            yield SkipReason(f"Inconsistent model versions detected: {versions}")
            return

        current_version = versions[0]
        last_triggered_version = context.cursor

        if last_triggered_version != current_version:
            context.log.info(
                f"All partitions persisted for version {current_version}. Triggering serving."
            )
            if job_partitions is None:
                context.log.info(
                    f"Triggering non-partitioned serving for version {current_version}"
                )
                # Non-partitioned job
                yield RunRequest(
                    run_key=current_version, tags={"model_version": current_version}
                )
            else:
                context.log.info(
                    f"Triggering partitioned serving for version {current_version}"
                )
                # Partitioned job
                for partition_key in job_partitions.get_partition_keys():
                    yield RunRequest(
                        run_key=f"{current_version}_{partition_key}",
                        partition_key=partition_key,
                        tags={
                            "model_version": current_version,
                            "level": partition_key,
                        },
                    )
            context.update_cursor(current_version)
        else:
            context.log.info(f"Version {current_version} already triggered. Skipping.")
            yield SkipReason(f"Version {current_version} already triggered. Skipping.")

    return _sensor
