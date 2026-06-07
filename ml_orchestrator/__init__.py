from dagster import Definitions, load_assets_from_modules

from .assets import mlops_data, mlops_registry, mlops_training, mlops_transformation
from .jobs import all_jobs
from .resources import duckdb_resource, local_storage_resource

# from .schedules import all_schedules
from .sensors import all_sensors

data_assets = load_assets_from_modules([mlops_data])
transformation_assets = load_assets_from_modules([mlops_transformation])
training_assets = load_assets_from_modules([mlops_training])
registry_assets = load_assets_from_modules([mlops_registry])

defs = Definitions(
    assets=[
        *data_assets,
        *transformation_assets,
        *training_assets,
        *registry_assets,
    ],
    jobs=all_jobs,
    # schedules=all_schedules,
    sensors=all_sensors,
    resources={**duckdb_resource, **local_storage_resource},
)
