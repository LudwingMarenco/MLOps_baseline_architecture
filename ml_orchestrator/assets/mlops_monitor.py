from utils.monitoring import MonitorServingQuality

from ..partitions import client_one_serving_partition
from . import mlops_constants

data_client_one_monitor = MonitorServingQuality(
    monitor_params=mlops_constants.churn_modeling_workflow_one,
    data_asset="data_client_one",
    serving_data_asset="data_client_one_monitor_data",
    asset_name="data_client_one_monitor",
    partition_level=client_one_serving_partition,
    group_name="churn_modeling_workflow_one_monitor",
).create_asset()
