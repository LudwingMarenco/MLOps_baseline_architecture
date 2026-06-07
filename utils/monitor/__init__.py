import json
import os
from datetime import datetime

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    MetadataValue,
    Output,
    PartitionsDefinition,
    asset,
)
from scipy import stats

from utils.parameters import get_parameters
from utils.serving import get_gto_info

STATUS_DISPLAY = {
    "RED": "🔴 RED",
    "AMBER": "🟡 AMBER",
    "GREEN": "🟢 GREEN",
}

status_rank = {"GREEN": 0, "AMBER": 1, "RED": 2}


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
            drift_amber = monitoring_cfg["drift_pvalue"]["amber"]
            drift_red = monitoring_cfg["drift_pvalue"]["red"]
            pred_amber = monitoring_cfg["prediction_drift"]["amber"]
            pred_red = monitoring_cfg["prediction_drift"]["red"]

            feature_cols = [c for c in serving_data.columns if c.startswith("C")]
            training_feature_cols = [
                c for c in training_data.columns if c.startswith("C")
            ]

            # Data Quality
            null_rate = serving_data[feature_cols].isnull().mean().mean()
            column_count = len(feature_cols)
            out_of_range = [
                col
                for col in feature_cols
                if col in training_feature_cols
                and (
                    serving_data[col].min() < training_data[col].min()
                    or serving_data[col].max() > training_data[col].max()
                )
            ]

            if null_rate >= null_red or column_count != len(training_feature_cols):
                dq_status = "RED"
            elif null_rate >= null_amber or out_of_range:
                dq_status = "AMBER"
            else:
                dq_status = "GREEN"

            # Feature Drift Kolmogorov-Smirnov (KS)
            drifted_features = []
            for col in feature_cols:
                if col not in training_feature_cols:
                    continue
                _, p_value = stats.ks_2samp(
                    training_data[col].dropna().values,
                    serving_data[col].dropna().values,
                )
                if p_value < drift_amber:
                    drifted_features.append(col)

            drift_rate = len(drifted_features) / len(feature_cols)

            if drift_rate >= drift_red:
                drift_status = "RED"
            elif drift_rate >= drift_amber:
                drift_status = "AMBER"
            else:
                drift_status = "GREEN"

            # Prediction Drift
            training_churn_rate = training_data["LABEL"].mean()
            batch_churn_rate = serving_data["PREDICTION"].mean()
            delta = abs(batch_churn_rate - training_churn_rate)

            if delta >= pred_red:
                pred_status = "RED"
            elif delta >= pred_amber:
                pred_status = "AMBER"
            else:
                pred_status = "GREEN"

            overall_status = max(
                [dq_status, drift_status, pred_status],
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
                        "out_of_range_columns": out_of_range,
                    },
                    "feature_drift": {
                        "status": drift_status,
                        "drifted_feature_count": len(drifted_features),
                        "drift_rate": round(float(drift_rate), 4),
                        "method": "ks_2samp",
                    },
                    "prediction_drift": {
                        "status": pred_status,
                        "training_churn_rate": round(float(training_churn_rate), 4),
                        "batch_churn_rate": round(float(batch_churn_rate), 4),
                        "delta": round(float(delta), 4),
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
                    "drift_status": MetadataValue.text(STATUS_DISPLAY[drift_status]),
                    "pred_status": MetadataValue.text(STATUS_DISPLAY[pred_status]),
                    "drifted_feature_count": MetadataValue.int(len(drifted_features)),
                    "null_rate": MetadataValue.float(round(float(null_rate), 4)),
                    "batch_churn_rate": MetadataValue.float(
                        round(float(batch_churn_rate), 4)
                    ),
                    "training_churn_rate": MetadataValue.float(
                        round(float(training_churn_rate), 4)
                    ),
                    "report_path": MetadataValue.text(report_path),
                },
            )

        return _asset
