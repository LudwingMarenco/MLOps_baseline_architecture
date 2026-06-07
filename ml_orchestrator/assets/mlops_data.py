from utils.data_fetcher import (
    DuckDBDataFetcher,
    DuckDBDynamicChunkedFetcher,
    DuckDBPartitionedDataFetcher,
)

from ..partitions import client_one_serving_partition
from . import mlops_constants

data_client_one = DuckDBDataFetcher(
    data_params=mlops_constants.churn_modeling_workflow_one,
    asset_name="data_client_one",
    group_name="churn_modeling_workflow_one",
    train=True,
).create_asset()

data_client_one_serving_data = DuckDBDynamicChunkedFetcher(
    data_params=mlops_constants.churn_modeling_workflow_one,
    op_name="data_client_one_serving_data",
).create_op()


data_client_one_monitor = DuckDBPartitionedDataFetcher(
    data_params=mlops_constants.churn_modeling_workflow_one,
    asset_name="data_client_one_monitor",
    partition_level=client_one_serving_partition,
    group_name="churn_modeling_workflow_one_monitor",
).create_asset()
