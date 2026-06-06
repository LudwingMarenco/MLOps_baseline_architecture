from utils.training import ClassifierModelTraining, EvaluateQualityTraining

from ..partitions import data_client_one_partition
from . import mlops_constants

data_client_one = ClassifierModelTraining(
    training_params=mlops_constants.churn_modeling_workflow_one,
    training_data_asset="data_client_one_train_data",
    asset_name="data_client_one_training",
    model_partitions=data_client_one_partition,
    group_name="churn_modeling_workflow_one",
).create_asset()

location_rating_training_quality = EvaluateQualityTraining(
    training_params=mlops_constants.churn_modeling_workflow_one,
    model_asset="data_client_one_training",
    data_input_asset="data_client_one_process_data",
    test_data_asset="data_client_one_test_data",
    asset_name="data_client_one_training_quality",
    model_partitions=data_client_one_partition,
    group_name="churn_modeling_workflow_one",
).create_asset()
