import inspect
import re

import numpy as np
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetSelection,
    DefaultSensorStatus,
    JobDefinition,
    MetadataValue,
    Output,
    PartitionsDefinition,
    ResourceParam,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    asset,
    sensor,
)
from scipy.stats import norm
from sklearn.metrics import accuracy_score, balanced_accuracy_score, mean_absolute_error
from sklearn.multioutput import MultiOutputClassifier
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.utils.class_weight import compute_sample_weight
from sklearn.utils.multiclass import type_of_target

from utils.artifacts import (
    classifiers,
    scalers,
)
from utils.parameters import (
    get_asset_metadata,
    get_latest_partition_materialization,
    get_parameters,
    validate_choice,
)
from utils.plot import plot_model_chara


class ClassifierModelTraining:
    def __init__(
        self,
        training_params: str,
        training_data_asset: str,
        asset_name: str,
        model_partitions: PartitionsDefinition,
        group_name: str,
        multioutput: bool = False,
    ) -> None:
        self.training_params = training_params
        self.training_data_asset = training_data_asset
        self.asset_name = asset_name
        self.model_partitions = model_partitions
        self.group_name = group_name
        self.multioutput = multioutput

    def create_asset(self) -> asset:
        @asset(
            name=self.asset_name,
            ins={"training_data": AssetIn(self.training_data_asset)},
            tags={"domain": "ML", "pii": "false"},
            deps=[self.training_data_asset],
            group_name=self.group_name,
            partitions_def=self.model_partitions,
            kinds={"python", "scikitlearn"},
        )
        def _asset(context: AssetExecutionContext, training_data) -> Output[Pipeline]:
            """
            Asset to train a classification model.
            """

            metadata = get_asset_metadata(context, asset_name=self.training_data_asset)
            parameters = get_parameters(self.training_params)
            parameters = parameters["training"]
            target_name = metadata["target"]
            model_option = context.partition_key
            model_option = re.search(r"(model_\d+)$", model_option).group(1)

            scaler = validate_choice(
                parameters[model_option]["scaler"], scalers, "scaler"
            )
            model_class = validate_choice(
                parameters[model_option]["model_name"], classifiers, "classifier"
            )
            if self.multioutput:
                model = MultiOutputClassifier(
                    model_class(**parameters[model_option]["params"])
                )
            else:
                model = model_class(**parameters[model_option]["params"])

            if scaler is None:
                pipeline = make_pipeline(model)
            else:
                pipeline = make_pipeline(scaler, model)

            targets = [target_name] if isinstance(target_name, str) else target_name
            target_train = training_data[target_name].values
            data_train = training_data.loc[
                :, ~training_data.columns.isin(targets)
            ].values

            step_name = list(pipeline.named_steps.keys())[-1]
            final_estimator = pipeline.named_steps[step_name]
            if isinstance(final_estimator, MultiOutputClassifier):
                base_estimator = final_estimator.estimator
            else:
                base_estimator = final_estimator

            try:
                supports_sample_weight = (
                    "sample_weight" in inspect.signature(base_estimator.fit).parameters
                )
            except (ValueError, TypeError):
                supports_sample_weight = False

            if parameters["sample_weights"] and supports_sample_weight:
                if target_train.ndim == 2 and target_train.shape[1] > 1:
                    y_for_weights = target_train[:, 0]
                else:
                    y_for_weights = target_train.ravel()
                sample_weights = compute_sample_weight("balanced", y_for_weights)
                fit_params = {f"{step_name}__sample_weight": sample_weights}
                pipeline.fit(data_train, target_train, **fit_params)
            else:
                pipeline.fit(data_train, target_train)

            return Output(
                pipeline,
                metadata={
                    "scaler": MetadataValue.text(parameters[model_option]["scaler"]),
                    "model": MetadataValue.text(parameters[model_option]["model_name"]),
                    "model_partition": MetadataValue.text(context.partition_key),
                    "task": MetadataValue.text("classification"),
                },
            )

        return _asset


class EvaluateQualityTraining:
    def __init__(
        self,
        training_params: str,
        model_asset: str,
        data_input_asset: str,
        test_data_asset: str,
        asset_name: str,
        model_partitions: PartitionsDefinition,
        group_name: str,
        figure: bool = True,
    ) -> None:
        self.training_params = training_params
        self.model_asset = model_asset
        self.data_input_asset = data_input_asset
        self.test_data_asset = test_data_asset
        self.asset_name = asset_name
        self.model_partitions = model_partitions
        self.group_name = group_name
        self.figure = figure

    def create_asset(self) -> asset:
        @asset(
            name=self.asset_name,
            ins={
                "model": AssetIn(self.model_asset),
                "data": AssetIn(self.data_input_asset),
                "test_data": AssetIn(self.test_data_asset),
            },
            tags={"domain": "ML", "pii": "false"},
            deps=[self.model_asset, self.data_input_asset, self.test_data_asset],
            partitions_def=self.model_partitions,
            group_name=self.group_name,
            kinds={"python", "scikitlearn"},
        )
        def _asset(
            context: AssetExecutionContext, model, data, test_data
        ) -> Output[float]:
            """
            Asset to evaluate quality of training.
            """
            parameters = get_parameters(self.training_params)
            parameters = parameters["training"]
            use_sample_weights = parameters["sample_weights"]
            asset_key = AssetKey(self.model_asset)
            model_partition = context.partition_key
            materialization = get_latest_partition_materialization(
                context, asset_key, model_partition
            )
            model_metadata = materialization.asset_materialization.metadata
            data_metadata = get_asset_metadata(context, self.data_input_asset)
            target_name = data_metadata["target"]
            task = model_metadata["task"].value
            time_series = (
                model_metadata.get("time_series").value
                if "time_series" in model_metadata
                else False
            )

            y_test = test_data[target_name].values
            targets = [target_name] if isinstance(target_name, str) else target_name
            x_test = test_data.loc[:, ~test_data.columns.isin(targets)].values

            if task == "classification":
                y_pred = model.predict(x_test)
                if y_test.ndim > 1 and y_test.shape[1] == 1:
                    y_test_eval = y_test.ravel()
                else:
                    y_test_eval = y_test
                target_type = type_of_target(y_test_eval)
                if use_sample_weights:
                    if target_type in ["binary", "multiclass"]:
                        accuracy = balanced_accuracy_score(y_test_eval, y_pred)
                    else:
                        accuracy = accuracy_score(y_test_eval, y_pred)
                else:
                    accuracy = accuracy_score(y_test_eval, y_pred)
            else:
                y_pred = model.predict(x_test)
                accuracy = mean_absolute_error(y_test, y_pred)
                if time_series:
                    _, p_value, sign_acc = self._pesaran_timmermann_test(y_test, y_pred)

            if self.figure:
                figure = plot_model_chara(
                    model=model,
                    data=data,
                    test_data=test_data,
                    target_name=target_name,
                    task=task,
                )
            else:
                figure = None

            if isinstance(target_name, list):
                metadata_text = MetadataValue.json(target_name)
            else:
                metadata_text = MetadataValue.text(target_name)

            if time_series:
                metadata = {
                    "model_accuracy": MetadataValue.float(float(accuracy)),
                    "p_value": MetadataValue.float(float(p_value)),
                    "sign_accuracy": MetadataValue.float(float(sign_acc)),
                    "model_plot": figure,
                    "model_partition": MetadataValue.text(model_partition),
                    "task": task,
                    "target": metadata_text,
                }
            else:
                metadata = {
                    "model_accuracy": MetadataValue.float(float(accuracy)),
                    "model_plot": figure,
                    "model_partition": MetadataValue.text(model_partition),
                    "task": task,
                    "target": metadata_text,
                }

            return Output(accuracy, metadata=metadata)

        return _asset

    def _pesaran_timmermann_test(
        self, y_true: np.ndarray, y_pred: np.ndarray
    ) -> tuple[float, float, float]:
        """
        Determines whether a forecast does a good job of predicting the change in direction of a time series
        """
        y_true = np.asarray(y_true)
        y_pred = np.asarray(y_pred)
        s_true = (y_true > 0).astype(int)
        s_pred = (y_pred > 0).astype(int)
        n = len(y_true)
        py = np.mean(s_true)
        pz = np.mean(s_pred)
        p = np.mean(s_true == s_pred)
        pe = py * pz + (1 - py) * (1 - pz)
        var = (pe * (1 - pe)) / n
        pt_stat = (p - pe) / np.sqrt(var)
        p_value = 2 * (1 - norm.cdf(abs(pt_stat)))

        return float(pt_stat), float(p_value), float(p)


def should_retrain(
    model_accuracy,
    target_accuracy,
    task,
    direction_accuracy=None,
    target_direction=None,
    p_value=None,
    alpha=0.05,
) -> bool:
    if model_accuracy is None:
        return True
    if task == "classification":
        return model_accuracy <= target_accuracy

    elif task == "regression":
        magnitude_good = model_accuracy < target_accuracy
        if direction_accuracy is not None and target_direction is not None:
            direction_good = direction_accuracy >= target_direction
        else:
            direction_good = True
        if p_value is not None:
            statistically_good = p_value < alpha
        else:
            statistically_good = True
        keep_model = magnitude_good and direction_good and statistically_good
        return not keep_model

    else:
        raise ValueError(f"Unknown task: {task}")


def conditional_training(
    training_params: str,
    job: JobDefinition,
    accuracy_model_asset: str,
    model_partitions: PartitionsDefinition,
    sensor_name: str,
):
    @sensor(
        name=sensor_name,
        job=job,
        minimum_interval_seconds=3600,
        default_status=DefaultSensorStatus.STOPPED,
        description="Trigger training if threshold accuracy condition is not met for each a model partition.",
    )
    def _sensor(context: AssetExecutionContext):

        parameters = get_parameters(training_params)["training"]
        asset_key = AssetKey(accuracy_model_asset)

        run_requests = []

        for partition in model_partitions.get_partition_keys():

            materialization = get_latest_partition_materialization(
                context, asset_key, partition
            )

            if materialization is None:
                context.log.info(f"No accuracy materialization found for {partition}")
            else:
                metadata = materialization.asset_materialization.metadata

                if "model_accuracy" not in metadata:
                    context.log.info(f"Accuracy metadata missing for {partition}.")
                else:
                    task = metadata["task"].value
                    model_accuracy = round(metadata["model_accuracy"].value, 4)
                    p_value = (
                        np.round(metadata["p_value"].value, 6)
                        if "p_value" in metadata
                        else None
                    )
                    direction_accuracy = (
                        np.round(metadata["sign_accuracy"].value, 4)
                        if "sign_accuracy" in metadata
                        else None
                    )
                    model_option = partition
                    model_option = re.search(r"(model_\d+)$", model_option).group(1)
                    target_accuracy = parameters[model_option]["target_accuracy"]
                    target_direction = (
                        parameters[model_option]["target_direction"]
                        if "target_direction" in parameters[model_option].keys()
                        else None
                    )

                    if should_retrain(
                        model_accuracy=model_accuracy,
                        target_accuracy=target_accuracy,
                        task=task,
                        direction_accuracy=direction_accuracy,
                        target_direction=target_direction,
                        p_value=p_value,
                    ):
                        context.log.info(
                            f"Accuracy: {model_accuracy}, Target: {target_accuracy}, "
                            f"Direction Accuracy: {direction_accuracy}, Direction Target: {target_direction}, "
                            f"p_value: {p_value}. Triggering retrain for partition {partition}."
                        )
                        run_requests.append(
                            RunRequest(
                                run_key=None,
                                partition_key=partition,
                            )
                        )
                    else:
                        context.log.info(
                            f"Accuracy: {model_accuracy}, Target: {target_accuracy}, "
                            f"Direction Accuracy: {direction_accuracy}, Direction Target: {target_accuracy}, "
                            f"p_value: {p_value}. Skipping retrain for partition {partition}."
                        )

        if run_requests:
            yield from run_requests
        else:
            yield SkipReason("No partitions require retraining at this time.")

    return _sensor


def training_based_tsp(
    name: str, group_name: str, model_partition: PartitionsDefinition
):
    @sensor(
        name=name,
        minimum_interval_seconds=86400,
        asset_selection=AssetSelection.groups(group_name),
        description="Trigger training if a new TSP version is available",
    )
    def _sensor(
        context: SensorEvaluationContext,
        snowflake_client: ResourceParam[SnowflakeResource],
    ):
        query = """
                SELECT DISTINCT(TSP_VERSION_NUM)
                FROM REALAI_PROD.PUBLIC.INT_PROPERTY_MFR
                WHERE TSP_VERSION_NUM IS NOT NULL
                ORDER BY TSP_VERSION_NUM DESC
                LIMIT 1;
                """

        with snowflake_client.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                data = cursor.fetchall()

            if not data:
                context.log.info("No TSP_VERSION_NUM found")
                return SkipReason("No TSP_VERSION_NUM found")

            latest_version = str(data[0][0])
            previous_version = context.cursor or None

            if previous_version is None:
                context.update_cursor(latest_version)
                return SkipReason(f"Initialized cursor with version {latest_version}")

            if latest_version == previous_version:
                return SkipReason(
                    f"No change detected in TSP_VERSION_NUM ({latest_version})"
                )

            context.update_cursor(latest_version)

            run_requests = []

            for partition_key in model_partition.get_partition_keys():
                run_requests.append(
                    RunRequest(
                        run_key=f"{latest_version}_{partition_key}",
                        partition_key=partition_key,
                        tags={
                            "TSP_VERSION_NUM": latest_version,
                            "MODEL": partition_key,
                        },
                    )
                )
            return run_requests

    return _sensor
