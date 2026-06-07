from ..assets.mlops_deployment import data_client_one_serving
from ..partitions import client_one_serving_partition

data_client_one_serving_job = data_client_one_serving.to_job(
    name="data_client_one_serving_job",
    description="Client One Serving",
    partitions_def=client_one_serving_partition,
)

all_jobs = [data_client_one_serving_job]
