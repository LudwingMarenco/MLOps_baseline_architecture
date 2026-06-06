import os
import re

import yaml
from dagster import (
    AssetExecutionContext,
    AssetKey,
    DagsterEventType,
    EventRecordsFilter,
)


def generate_keys(model_params: str, asset_name: str) -> list:
    parameters = get_parameters(model_params)
    model_options = [
        key for key in parameters["training"].keys() if re.match(r"^model_\d+$", key)
    ]
    return [f"{asset_name}_{key}" for key in model_options]


def get_parameters(filename: str):

    filename = os.path.join("ml_orchestrator/config/", filename)

    with open(filename, "r") as file:
        config = yaml.safe_load(file)

    validation_rules = {
        "params": {"data", "training", "serving"},
    }

    for key, required_fields in validation_rules.items():
        if key in filename:
            missing_fields = required_fields - config.keys()
            if missing_fields:
                raise ValueError(
                    f"Missing required fields for parameters file: {missing_fields}"
                )
        else:
            raise ValueError(
                f"Parameters filename must contain {validation_rules.keys()} suffix"
            )
    return config


def validate_choice(choice, valid_options, name):
    if choice not in valid_options:
        raise ValueError(
            f"Unsupported {name}: {choice}. Supported {name}s: {list(valid_options.keys())}"
        )
    else:
        return valid_options[choice]


def get_asset_metadata(context: AssetExecutionContext, asset_name: str):
    latest_event = context.instance.get_latest_materialization_event(
        AssetKey(asset_name)
    )

    if (
        latest_event
        and latest_event.dagster_event
        and latest_event.dagster_event.event_type_value == "ASSET_MATERIALIZATION"
    ):
        materialization = (
            latest_event.dagster_event.step_materialization_data.materialization
        )

        if materialization and materialization.metadata:
            metadata_dict = {
                key: value.value for key, value in materialization.metadata.items()
            }
        else:
            metadata_dict = {}
    else:
        metadata_dict = {}
    return metadata_dict


def get_latest_partition_materialization(context, asset_key, partition_key):
    records = context.instance.get_event_records(
        EventRecordsFilter(
            asset_key=asset_key,
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_partitions=[partition_key],
        ),
        limit=1,
    )
    return records[0] if records else None
