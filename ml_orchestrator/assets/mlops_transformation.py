import pandas as pd
from dagster import AssetExecutionContext, AssetIn, MetadataValue, Output, asset

from utils.data_processing import TrainTestDataAsset

from ..partitions import data_client_one_partition
from . import mlops_constants


@asset(
    ins={"data": AssetIn("data_client_one")},
    tags={"domain": "ML", "pii": "false"},
    group_name="churn_modeling_workflow_one",
    kinds={"python", "pandas"},
)
def data_client_one_process_data(
    context: AssetExecutionContext, data
) -> Output[pd.DataFrame]:
    """
    Asset to process data coming from data_client_one.
    """
    rows_before = len(data)
    data = data.dropna()
    data = data.drop(columns="ROW_ID")
    rows_dropped = rows_before - len(data)
    context.log.info(f"Dropped {rows_dropped} rows with null values")

    if isinstance(mlops_constants.client_one_targets, list):
        target = MetadataValue.json(mlops_constants.client_one_targets)
    else:
        target = MetadataValue.text(mlops_constants.client_one_targets)

    return Output(
        data,
        metadata={
            "n_samples": MetadataValue.int(data.shape[0]),
            "n_features": MetadataValue.int(data.shape[1]),
            "rows_dropped": MetadataValue.int(rows_dropped),
            "target": target,
            "include_time": MetadataValue.bool(False),
        },
    )


data_client_one_splitting_data = TrainTestDataAsset(
    split_params=mlops_constants.churn_modeling_workflow_one,
    input_data_asset="data_client_one_process_data",
    asset_name="data_client_one_splitting_data",
    train_asset_name="data_client_one_train_data",
    test_asset_name="data_client_one_test_data",
    model_partitions=data_client_one_partition,
    group_name="churn_modeling_workflow_one",
).create_asset()
