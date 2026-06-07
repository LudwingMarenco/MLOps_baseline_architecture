from dagster import graph

# from mlops_constants import churn_modeling_workflow_one
from ..assets.mlops_data import data_client_one_serving_data

# from ..partitions import data_client_one_partition

# from utils.serving import ModelChunkedServing


# data_client_one_chunked_serving = ModelChunkedServing(
#     serving_params=churn_modeling_workflow_one,
#     artifact_model_asset="data_client_one_artifact",
#     accuracy_model_asset="data_client_one_training_quality",
#     model_partitions=data_client_one_partition,
#     op_name="data_client_one_chunked_serving",
# ).create_op()


@graph
def data_client_one_serving():
    chunks = data_client_one_serving_data()
    # processed_chunks = chunks.map(area_median_transform_data)
    # processed_chunks.map(data_client_one_chunked_serving)
    # processed_chunks.map(data_client_one_chunked_serving)
