from utils.persistence import SaveMLArtifact

from ..partitions import data_client_one_partition
from . import mlops_constants

data_client_one_artifact = SaveMLArtifact(
    model_params=mlops_constants.churn_modeling_workflow_one,
    model_asset="data_client_one_training",
    model_quality_asset="data_client_one_training_quality",
    asset_name="data_client_one_artifact",
    model_partitions=data_client_one_partition,
    group_name="churn_modeling_workflow_one",
).create_asset()
