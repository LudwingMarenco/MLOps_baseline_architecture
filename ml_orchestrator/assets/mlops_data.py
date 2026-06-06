from utils.data_fetcher import DuckDBDataFetcher

from . import mlops_constants

# ------------ area median income ---------------------

area_median_data = DuckDBDataFetcher(
    data_params=mlops_constants.churn_modeling_workflow,
    asset_name="churn_modeling_workflow_data",
    group_name="churn_modeling_workflow",
    train=True,
).create_asset()
