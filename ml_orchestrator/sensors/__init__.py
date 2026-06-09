from utils.monitoring import conditional_monitoring, conditional_retraining
from utils.serving import conditional_serving
from utils.training import conditional_training

from ..assets.mlops_constants import churn_modeling_workflow_one
from ..jobs import (
    data_client_one_monitoring_job,
    data_client_one_retraining_job,
    data_client_one_serving_job,
    data_client_one_training_job,
)
from ..partitions import (
    client_one_serving_partition,
    data_client_one_partition,
)

data_client_one_training_sensor = conditional_training(
    training_params=churn_modeling_workflow_one,
    job=data_client_one_training_job,
    accuracy_model_asset="data_client_one_training_quality",
    model_partitions=data_client_one_partition,
    sensor_name="data_client_one_training_sensor",
)

data_client_one_serving_sensor = conditional_serving(
    job=data_client_one_serving_job,
    artifact_model_asset="data_client_one_artifact",
    model_partitions=data_client_one_partition,
    sensor_name="data_client_one_serving_sensor",
    job_partitions=client_one_serving_partition,
)

data_client_one_monitoring_sensor = conditional_monitoring(
    job=data_client_one_monitoring_job,
    model_partitions=client_one_serving_partition,
    sensor_name="data_client_one_monitoring_sensor",
)

data_client_one_retraining = conditional_retraining(
    job=data_client_one_retraining_job,
    model_partitions=data_client_one_partition,
    level_partitions=client_one_serving_partition,
    sensor_name="data_client_one_retraining",
)

all_sensors = [
    data_client_one_training_sensor,
    data_client_one_serving_sensor,
    data_client_one_monitoring_sensor,
    data_client_one_retraining,
]
