from dagster import (
    PartitionsDefinition,
    StaticPartitionsDefinition,
)

from utils.parameters import generate_keys


def static_partition_from_parameters(
    model_params: str, model_asset_name: str
) -> PartitionsDefinition:
    names = generate_keys(model_params, model_asset_name)
    return StaticPartitionsDefinition(names)
