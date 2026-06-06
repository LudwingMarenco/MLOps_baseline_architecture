from utils.partitions import static_partition_from_parameters

from ..assets.mlops_constants import churn_modeling_workflow_one

data_client_one_partition = static_partition_from_parameters(
    model_params=churn_modeling_workflow_one, model_asset_name="data_client_one"
)
