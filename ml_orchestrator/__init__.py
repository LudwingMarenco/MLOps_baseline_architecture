from dagster import Definitions, load_assets_from_modules

from .assets import (  # mlops_deployment,; mlops_registry,; mlops_training,; mlops_utils,
    mlops_data,
    mlops_transformation,
)

# from .jobs import all_jobs
from .resources import duckdb_resource

# from .schedules import all_schedules
# from .sensors import all_sensors

data_assets = load_assets_from_modules([mlops_data])
transformation_assets = load_assets_from_modules([mlops_transformation])
# training_assets = load_assets_from_modules([mlops_training])
# deployment_assets = load_assets_from_modules([mlops_deployment])
# registry_assets = load_assets_from_modules([mlops_registry])
# utils_assets = load_assets_from_modules([mlops_utils])


defs = Definitions(
    assets=[
        *data_assets,
        *transformation_assets,
        # *training_assets,
        # *registry_assets,
        # *deployment_assets,
        # *utils_assets,
    ],
    # jobs=all_jobs,
    # schedules=all_schedules,
    # sensors=all_sensors,
    resources={**duckdb_resource},
)
