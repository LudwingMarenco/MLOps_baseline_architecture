The model serving job, which follows a chunked processing approach, is designed to efficiently apply an ensemble of 
models to large datasets. However, this serving stage should only be triggered under specific conditions within 
the asset pipelines associated with machine learning training. In particular, serving must wait until the ensemble 
has been successfully registered, meaning, models of the ensemble were persisted.

To support this behavior, a ready-to-use sensor has been implemented to automatically trigger the serving job when 
the models trained by the asset pipeline reach or exceed the predefined target accuracy specified in the 
target_accuracy` field. For ensembles, this condition is evaluated across all models, ensuring that serving is 
only activated once the ensemble meets the required performance criteria.

The core idea is that the final asset in each training pipeline is responsible for persisting its corresponding 
model. This step is performed using the `SaveMLArtifact` asset object, which includes a persisted boolean metadata 
flag. The serving job is triggered only after all models in the ensemble have been successfully persisted, that is, 
once each model’s accuracy meets or exceeds the target threshold.

Detailed instructions for creating and using this sensor are provided below.

## Create Ensemble Model Partition

In `ml_orchestrator/partitions/__init__.py`, use the `static_partition_from_parameters` function to automatically create 
the ensemble partition based on the configuration file. This function is located in the `utils/partitions` subfolder.

```python3 title="ml_orchestrator/partitions/__init__.py"
from . import mlops_constants
from utils.partitions import static_partition_from_parameters

workflow_example_partitions = static_partition_from_parameters(model_params=mlops_constants.workflow_example,
                                                        model_asset_name="workflow_example")
```

## Create the Conditional Serving Sensor

To create the conditional serving sensor, import the `conditional_serving` function from the `utils/serving` directory
into `ml_orchestrator/sensors/__init__.py`

### Input Parameters

* **job** (JobDefinition): Job associated with the graph that includes the operation function for chunked model serving.
* **artifact_model_assets** (str): Model artifact asset name to be used in the serving process.
* **model_partitions** (PartitionsDefinition): Dagster Partition definition containing information of ensemble of models.
* **sensor_name** (str): Name of the sensor used for logging and visualization purposes.

```python3 title="ml_orchestrator/sensors/__init__.py"
from ..jobs import model_serving_job
from utils.serving import conditional_serving
from ..partitions import workflow_example_partitions




conditional_serving_sensor = conditional_serving(job=model_serving_job,
                                                 artifact_model_asset="model_asset",
                                                 model_partitions= workflow_example_partitions,
                                                 sensor_name="conditional_serving_sensor")
```

This sensor checks whether all specified model artifacts have been persisted. If all models meet the persistence
condition (i.e., accuracy meets or exceeds the target threshold), it triggers the model serving job. The persistence 
condition is checked every 12 hours.

