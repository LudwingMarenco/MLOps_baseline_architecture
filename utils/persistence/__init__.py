import io
import json
import os
import re
import subprocess

import joblib
import numpy as np
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetSelection,
    EnvVar,
    MetadataValue,
    Output,
    PartitionsDefinition,
    ResourceParam,
    asset,
    define_asset_job,
)

from ml_orchestrator.resources import LocalStorageResource
from utils.parameters import get_latest_partition_materialization, get_parameters


def is_model_good_enough(
    task: str,
    metric_value: float,
    threshold: float,
    direction_value=None,
    direction_threshold=None,
    p_value=None,
    alpha=0.05,
) -> bool:
    if metric_value is None:
        return False

    if task == "classification":
        return metric_value >= threshold

    if task == "regression":
        is_good = metric_value <= threshold
        if direction_value is not None and direction_threshold is not None:
            is_good = is_good and (direction_value >= direction_threshold)
        if p_value is not None:
            is_good = is_good and (p_value <= alpha)
        return is_good

    # TODO: Add value comparison for data reducers
    if task == "data_reduction":
        return True

    return False


class SaveMLArtifact:
    def __init__(
        self,
        model_params: str,
        model_asset: str,
        model_quality_asset: str,
        asset_name: str,
        model_partitions: PartitionsDefinition,
        group_name: str,
    ) -> None:
        self.model_params = model_params
        self.model_asset = model_asset
        self.model_quality_asset = model_quality_asset
        self.asset_name = asset_name
        self.model_partitions = model_partitions
        self.group_name = group_name

    def create_asset(self) -> asset:
        @asset(
            name=self.asset_name,
            ins={
                "model": AssetIn(self.model_asset),
                "quality_model": AssetIn(self.model_quality_asset),
            },
            tags={"domain": "ML", "pii": "false"},
            deps=[self.model_asset, self.model_quality_asset],
            partitions_def=self.model_partitions,
            group_name=self.group_name,
            kinds={"python", "local"},
        )
        def _asset(
            context: AssetExecutionContext,
            model,
            quality_model,
            model_persistor: ResourceParam[LocalStorageResource],
        ) -> Output[None]:
            """
            Asset to persist trained model.
            """

            model_name = context.partition_key
            model_option = context.partition_key
            model_asset_key = AssetKey(self.model_asset)
            model_materialization = get_latest_partition_materialization(
                context, model_asset_key, model_option
            )
            model_metadata = model_materialization.asset_materialization.metadata
            task = model_metadata["task"].value

            accu_asset_key = AssetKey(self.model_quality_asset)
            accu_materialization = get_latest_partition_materialization(
                context, accu_asset_key, model_option
            )
            accu_metadata = accu_materialization.asset_materialization.metadata
            accuracy = accu_metadata["model_accuracy"].value
            direction_accuracy = (
                np.round(accu_metadata["sign_accuracy"].value, 4)
                if "sign_accuracy" in accu_metadata
                else None
            )
            p_value = (
                np.round(accu_metadata["p_value"].value, 6)
                if "p_value" in accu_metadata
                else None
            )
            target_name = accu_metadata["target"].value

            if model_metadata["model_partition"].value != context.partition_key:
                raise RuntimeError(
                    "Model metadata partition does not match context partition"
                )

            if accu_metadata["model_partition"].value != context.partition_key:
                raise RuntimeError(
                    "Quality metadata partition does not match context partition"
                )

            parameters = get_parameters(self.model_params)

            model_option = re.search(r"(model_\d+)$", model_option).group(1)
            threshold = parameters["training"][model_option]["target_accuracy"]
            direction_threshold = (
                parameters["training"][model_option]["target_direction"]
                if "target_direction" in parameters["training"][model_option].keys()
                else None
            )
            artifact_name = model_name + ".pkl"

            persisted = is_model_good_enough(
                task,
                accuracy,
                threshold,
                direction_accuracy,
                direction_threshold,
                p_value,
            )

            buffer = io.BytesIO()
            joblib.dump(model, buffer)
            buffer.seek(0)
            artifact_path = model_persistor.save(artifact_name, buffer)

            if persisted:
                model_version = model_persistor.register(model_name)
            else:
                model_version = model_persistor.get_version(model_name)

            if isinstance(target_name, list):
                metadata_text = MetadataValue.json(target_name)
            else:
                metadata_text = MetadataValue.text(target_name)

            return Output(
                None,
                metadata={
                    "artifact_path": MetadataValue.text(artifact_path),
                    "model_partition": MetadataValue.text(model_name),
                    "model_version": MetadataValue.text(model_version),
                    "task": MetadataValue.text(task),
                    "target": metadata_text,
                    "persisted": MetadataValue.bool(persisted),
                },
            )

        return _asset


def get_model_info(model_name: str, base_path: str = "models") -> tuple[str, str]:
    version_path = os.path.join(base_path, f"{model_name}_version.txt")

    if not os.path.exists(version_path):
        return "unknown"

    with open(version_path, "r") as f:
        version = f.read().strip()

    return version


def create_training_job(
    job_name: str,
    select_from_asset: str,
    model_partitions: PartitionsDefinition,
    group_name: str,
    description: str,
):

    base = AssetSelection.keys(select_from_asset)
    selection = (base.downstream() - base) & AssetSelection.groups(group_name)

    return define_asset_job(
        name=job_name,
        selection=selection,
        partitions_def=model_partitions,
        description=description,
    )
