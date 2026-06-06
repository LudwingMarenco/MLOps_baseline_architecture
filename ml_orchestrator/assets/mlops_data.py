from utils.data_fetcher import DuckDBDataFetcher

from . import mlops_constants

data_client_one = DuckDBDataFetcher(
    data_params=mlops_constants.churn_modeling_workflow_one,
    asset_name="data_client_one",
    group_name="churn_modeling_workflow_one",
    train=True,
).create_asset()
