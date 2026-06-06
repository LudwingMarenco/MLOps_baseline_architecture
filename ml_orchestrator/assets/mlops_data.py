from . import mlops_constants

# ------------ area median income ---------------------

area_median_data = SnowflakeDataFetcher(
    data_params=mlops_constants.area_median_params,
    asset_name="area_median_data",
    group_name="area_median_income",
    train=True,
).create_asset()
