from dagster import AssetSelection, define_asset_job

from utils.persistence import create_training_job

from ..assets.mlops_deployment import data_client_one_serving
from ..partitions import client_one_serving_partition, data_client_one_partition

data_client_one_training_job = create_training_job(
    job_name="data_client_one_training_job",
    select_from_asset="data_client_one_process_data",
    model_partitions=data_client_one_partition,
    group_name="churn_modeling_workflow_one",
    description="Conditional training for Client One",
)

data_client_one_serving_job = data_client_one_serving.to_job(
    name="data_client_one_serving_job",
    description="Client One Serving",
    partitions_def=client_one_serving_partition,
)

data_client_one_monitoring_job = define_asset_job(
    name="churn_modeling_workflow_one_monitor_job",
    selection=AssetSelection.groups("churn_modeling_workflow_one_monitor"),
)

data_client_one_retraining_job = define_asset_job(
    name="data_client_one_retraining_job",
    selection=AssetSelection.groups("churn_modeling_workflow_one"),
)

all_jobs = [
    data_client_one_training_job,
    data_client_one_serving_job,
    data_client_one_monitoring_job,
    data_client_one_retraining_job,
]
