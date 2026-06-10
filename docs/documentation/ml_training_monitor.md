In the previous section, we detailed the asset development process for a machine learning workflow and explained how to
use a set of pre-implemented, reusable assets. This section focuses on monitoring the training of machine learning
models.

The key idea is that the training parameters provided to Dagster through the parameter file are obtained from an offline
hyperparameter optimization process. However, because model training involves inherent randomness when executed within
Dagster, the resulting accuracy may differ slightly from the accuracy observed during offline optimization.

To address this issue, a reusable sensor has been implemented to automatically trigger model training runs until the
observed accuracy matches or exceeds the best accuracy value obtained during offline optimization. The sensor can be
found in in the `utils/training/` directory and is named as `create_training_sensors`. One sensor is create per ML workflow. See below for instructions in how to set it up and integrate it into the ML
workflow.

## Create Ensemble Model Partition

In `ml_orchestrator/partitions/__init__.py`, use the `static_partition_from_parameters` function to automatically create 
the ensemble partition based on the configuration file. This function is located in the `utils/partitions` subfolder.

```python3 title="ml_orchestrator/partitions/__init__.py"
from . import mlops_constants
from utils.partitions import static_partition_from_parameters

workflow_example_partitions = static_partition_from_parameters(model_params=mlops_constants.workflow_example,
                                                        model_asset_name="workflow_example")
```

## Create the Monitoring Job

In `ml_orchestrator/jobs/__init__.py`, use the `create_training_job` function to define a job that specifies which 
assets will be materialized when the training sensor is triggered. This function is located in `utils/persistence` directory.

### Input Parameters

* **job_name** (str): Name of the job containing assets to be materialized.
* **select_from_asset** (str): Name of the asset from which the job will select downstream assets.
* **model_partitions** (PartitionsDefinition): Dagster Partition definition containing information of ensemble of models.
* **group_name** (str): Name of the ML workflow group where the model evaluation is going to be evaluated.
* **description** (str): Description of the job’s purpose.


```python3 title="ml_orchestrator/jobs/__init__.py"
from utils.persistence import create_training_job
from ..partitions import workflow_example_partitions


model_training_job = create_training_job(job_name="model_training_job",
                                       select_from_asset="process_data",
                                       model_partitions= workflow_example_partitions,
                                       group_name="ml_task",
                                       description="Conditional training")
```

In this example, the job selects all assets within the `ml_task` group that are downstream of the `process_data asset. 
Assets outside this selection will not be materialized when the sensor triggers the job.

## Create the Conditional Training Sensor

In `ml_orchestrator/sensors/__init__.py` import the `conditional_training` function located in `utils/training` directory. 
This sensor monitors the training process for the entire model ensemble at once. The input parameters are described below.
### Input Parameters

* **training_params** (str): Path to the YAML file containing training configuration.
* **job** (str): Name of the job containing assets to be materialized.
* **accuracy_model_asset** (str): Name of the asset who evaluated quality of trained model.
* **model_partitions** (PartitionsDefinition): Dagster Partition definition containing information of ensemble of models.
* **sensor_name** (str): Name of the sensor used for logging and visualization purposes.

```python3 title="ml_orchestrator/sensors/__init__.py"
from ..jobs import model_training_job
from utils.training import conditional_training
from ..partitions import workflow_example_partitions
from ..assets.mlops_constants import workflow_example

conditional_training_sensor = conditional_training(training_params=workflow_example,
                                                   job=model_training_job,
                                                   accuracy_model_asset="model_quality",
                                                   model_partitions=workflow_example_partitions,
                                                   sensor_name="conditional_model_sensor")
```

This function retrieves the `target_accuracy` value and checks every one hour to check whether the trained model's
accuracy meets or exceeds the target.