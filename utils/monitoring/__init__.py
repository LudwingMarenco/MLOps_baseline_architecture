import glob
import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    DefaultSensorStatus,
    JobDefinition,
    MetadataValue,
    Output,
    PartitionsDefinition,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    asset,
    sensor,
)
from scipy import stats
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score

from utils.parameters import get_parameters
from utils.serving import get_gto_info

STATUS_DISPLAY = {
    "RED": "🔴 RED",
    "AMBER": "🟡 AMBER",
    "GREEN": "🟢 GREEN",
}

status_rank = {"GREEN": 0, "AMBER": 1, "RED": 2}

KS_SIGNIFICANCE = 0.05


class MonitorServingQuality:
    def __init__(
        self,
        monitor_params: str,
        data_asset: str,
        serving_data_asset: str,
        asset_name: str,
        partition_level: PartitionsDefinition,
        group_name: str,
    ) -> None:
        self.monitor_params = monitor_params
        self.data_asset = data_asset
        self.serving_data_asset = serving_data_asset
        self.asset_name = asset_name
        self.partition_level = partition_level
        self.group_name = group_name

    def create_asset(self):
        @asset(
            name=self.asset_name,
            tags={"domain": "ML", "pii": "false"},
            ins={
                "training_data": AssetIn(self.data_asset),
                "serving_data": AssetIn(self.serving_data_asset),
            },
            partitions_def=self.partition_level,
            group_name=self.group_name,
            kinds={"python", "scipy"},
        )
        def _asset(
            context: AssetExecutionContext,
            serving_data: pd.DataFrame,
            training_data: pd.DataFrame,
        ) -> Output[None]:
            """
            Asset to monitor data quality, feature drift and prediction drift
            for a given batch level.
            """

            parameters = get_parameters(self.monitor_params)
            monitoring_cfg = parameters["monitor"]
            level = context.partition_key

            null_amber = monitoring_cfg["null_rate"]["amber"]
            null_red = monitoring_cfg["null_rate"]["red"]
            drift_amber = monitoring_cfg["feature_drift"]["amber"]
            drift_green = monitoring_cfg["feature_drift"]["green"]
            pred_amber = monitoring_cfg["prediction_drift"]["amber"]
            pred_green = monitoring_cfg["prediction_drift"]["green"]
            perf_threshold_green = monitoring_cfg["model_performance"]["green"]
            perf_threshold_amber = monitoring_cfg["model_performance"]["amber"]

            feature_cols = [c for c in serving_data.columns if c.startswith("C")]
            training_feature_cols = [
                c for c in training_data.columns if c.startswith("C")
            ]

            # Data Quality
            null_rate = serving_data[feature_cols].isnull().mean().mean()
            column_count = len(feature_cols)

            if null_rate >= null_red or column_count != len(training_feature_cols):
                dq_status = "RED"
            elif null_rate >= null_amber:
                dq_status = "AMBER"
            else:
                dq_status = "GREEN"

            # Feature Drift - Kolmogorov-Smirnov (KS)
            drifted_features = []
            for col in feature_cols:
                if col not in training_feature_cols:
                    continue
                _, p_value = stats.ks_2samp(
                    training_data[col].dropna().values,
                    serving_data[col].dropna().values,
                )
                if p_value < KS_SIGNIFICANCE:
                    drifted_features.append(col)

            drift_rate = len(drifted_features) / len(feature_cols)

            if drift_rate <= drift_green:
                drift_status = "GREEN"
            elif drift_rate <= drift_amber:
                drift_status = "AMBER"
            else:
                drift_status = "RED"

            # Prediction Drift - Population Stability Index
            training_churn = training_data["LABEL"].dropna().values
            batch_churn = serving_data["PREDICTION"].dropna().values

            psi = calculate_psi(training_churn, batch_churn, buckettype="quantiles")

            if psi >= pred_amber:
                pred_status = "RED"
            elif psi >= pred_green:
                pred_status = "AMBER"
            else:
                pred_status = "GREEN"

            y_true = serving_data["LABEL"].values
            y_pred = serving_data["PREDICTION"].values

            accuracy = accuracy_score(y_true, y_pred)
            f1 = f1_score(y_true, y_pred)
            auc = roc_auc_score(y_true, y_pred)

            perf_threshold_green = monitoring_cfg["model_performance"]["green"]
            perf_threshold_amber = monitoring_cfg["model_performance"]["amber"]

            if accuracy >= perf_threshold_green:
                perf_status = "GREEN"
            elif accuracy >= perf_threshold_amber:
                perf_status = "AMBER"
            else:
                perf_status = "RED"

            overall_status = max(
                [dq_status, drift_status, pred_status, perf_status],
                key=lambda s: status_rank[s],
            )

            model_version, model_stage = get_gto_info(level)
            report = {
                "batch_id": level,
                "run_timestamp": datetime.now().isoformat(),
                "model_version": model_version,
                "model_stage": model_stage,
                "status": overall_status,
                "checks": {
                    "data_quality": {
                        "status": dq_status,
                        "null_rate": round(float(null_rate), 4),
                        "column_count": column_count,
                        "method": "Null values check",
                    },
                    "feature_drift": {
                        "status": drift_status,
                        "drifted_feature_count": len(drifted_features),
                        "drift_rate": round(float(drift_rate), 4),
                        "method": " Kolmogorov - Smirnov Test",
                    },
                    "prediction_drift": {
                        "status": pred_status,
                        "psi": round(float(psi), 4),
                        "method": "Drift Population Stability Index Test",
                    },
                    "model_performance": {
                        "status": perf_status,
                        "accuracy": round(float(accuracy), 4),
                        "f1_score": round(float(f1), 4),
                        "auc": round(float(auc), 4),
                        "method": "Ground Truth Comparison",
                    },
                },
            }

            output_dir = os.path.join(monitoring_cfg["output_path"], level)
            os.makedirs(output_dir, exist_ok=True)
            report_path = os.path.join(output_dir, "monitoring_report.json")
            with open(report_path, "w") as f:
                json.dump(report, f, indent=2)

            context.log.info(f"Monitoring status for {level}: {overall_status}")

            return Output(
                None,
                metadata={
                    "status": MetadataValue.text(STATUS_DISPLAY[overall_status]),
                    "dq_status": MetadataValue.text(STATUS_DISPLAY[dq_status]),
                    "feat_status": MetadataValue.text(STATUS_DISPLAY[drift_status]),
                    "pred_status": MetadataValue.text(STATUS_DISPLAY[pred_status]),
                    "perf_status": MetadataValue.text(STATUS_DISPLAY[perf_status]),
                    "report_path": MetadataValue.text(report_path),
                },
            )

        return _asset


def calculate_psi(expected, actual, buckettype="quantiles", buckets=10, axis=0):
    def psi(expected_array, actual_array, buckets):
        def scale_range(input, min, max):
            input += -(np.min(input))
            input /= np.max(input) / (max - min)
            input += min
            return input

        breakpoints = np.arange(0, buckets + 1) / (buckets) * 100

        if buckettype == "bins":
            breakpoints = scale_range(
                breakpoints, np.min(expected_array), np.max(expected_array)
            )
        elif buckettype == "quantiles":
            breakpoints = np.stack(
                [np.percentile(expected_array, b) for b in breakpoints]
            )

        expected_fractions = np.histogram(expected_array, breakpoints)[0] / len(
            expected_array
        )
        actual_fractions = np.histogram(actual_array, breakpoints)[0] / len(
            actual_array
        )

        def sub_psi(e_perc, a_perc):
            if a_perc == 0:
                a_perc = 0.0001
            if e_perc == 0:
                e_perc = 0.0001

            value = (e_perc - a_perc) * np.log(e_perc / a_perc)
            return value

        psi_value = sum(
            sub_psi(expected_fractions[i], actual_fractions[i])
            for i in range(0, len(expected_fractions))
        )

        return psi_value

    if len(expected.shape) == 1:
        psi_values = np.empty(len(expected.shape))
    else:
        psi_values = np.empty(expected.shape[1 - axis])

    for i in range(0, len(psi_values)):
        if len(psi_values) == 1:
            psi_values = psi(expected, actual, buckets)
        elif axis == 0:
            psi_values[i] = psi(expected[:, i], actual[:, i], buckets)
        elif axis == 1:
            psi_values[i] = psi(expected[i, :], actual[i, :], buckets)

    return psi_values


def conditional_monitoring(
    job: JobDefinition,
    model_partitions: PartitionsDefinition,
    sensor_name: str,
):
    @sensor(
        name=sensor_name,
        job=job,
        minimum_interval_seconds=180,
        default_status=DefaultSensorStatus.STOPPED,
        description="Trigger monitoring pipeline",
    )
    def _sensor(context: SensorEvaluationContext):
        base_path = "data/predictions"
        run_requests = []
        skipped_partitions = []

        for partition in model_partitions.get_partition_keys():
            if glob.glob(os.path.join(base_path, partition, "*.parquet")):
                run_requests.append(
                    RunRequest(
                        run_key=None,
                        partition_key=partition,
                    )
                )
            else:
                skipped_partitions.append(partition)

        if skipped_partitions:
            context.log.info(
                f"No predictions found for partitions: {', '.join(skipped_partitions)}"
            )

        if run_requests:
            yield from run_requests
        else:
            yield SkipReason(
                f"No prediction parquet files found in any partition: {', '.join(skipped_partitions)}"
            )

    return _sensor


def conditional_retraining(
    job: JobDefinition,
    model_partitions: PartitionsDefinition,
    level_partitions: PartitionsDefinition,
    sensor_name: str,
):
    @sensor(
        name=sensor_name,
        job=job,
        minimum_interval_seconds=900,
        default_status=DefaultSensorStatus.STOPPED,
        description="Trigger retraining pipeline when model version changes across all batches",
    )
    def _sensor(context: SensorEvaluationContext):
        base_path = "data/predictions"
        versions = {}

        for partition in level_partitions.get_partition_keys():
            metadata_files = sorted(
                glob.glob(os.path.join(base_path, partition, "*_metadata.json"))
            )

            if not metadata_files:
                yield SkipReason(f"No metadata files found for partition {partition}.")
                return

            with open(metadata_files[-1], "r") as f:
                metadata = json.load(f)

            current_version = metadata.get("model_version")

            if current_version is None:
                yield SkipReason(
                    f"No model version found in metadata for partition {partition}."
                )
                return

            versions[partition] = current_version

        # check all level partitions agree on the same version
        unique_versions = set(versions.values())
        if len(unique_versions) > 1:
            yield SkipReason(
                f"Model versions are inconsistent across partitions: {versions}. "
                f"Waiting for all batches to update."
            )
            return

        current_version = unique_versions.pop()
        last_seen_version = context.cursor

        if last_seen_version != current_version:
            context.log.info(
                f"Model version changed across all partitions: "
                f"{last_seen_version} -> {current_version}. Triggering retraining."
            )
            context.update_cursor(current_version)
            for model_partition in model_partitions.get_partition_keys():
                yield RunRequest(
                    run_key=f"{current_version}_{model_partition}",
                    partition_key=model_partition,
                )
        else:
            yield SkipReason(f"Model version unchanged: {current_version}")

    return _sensor
