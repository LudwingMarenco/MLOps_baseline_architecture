This section provides a detailed explanation of how ML asset development should be carried out to ensure an organized
and consistent asset format across the entire ML workflow. Additionally, it presents a set of pre-implemented assets
that can be used to support each stage of the ML lifecycle.

## Asset Mandatory Structure

To ensure consistency, traceability, and reusability across all machine learning workflows, all assets developed must
adhere to a standardized structure regardless of its internal logic or assigned group. The key components of this
standardization are outlined below:

* **Asset Tags**: Every asset must include a `tags` dictionary in its decorator with the following required fields:

    * **"domain"**: Specifies the relevant domain; in this case, the default value is `"ML"`.
    * **"pii"**: A flag indicating whether the asset contains personally identifiable information (`"true"` or
      `"false"`).

    * These tags serve as metadata for categorization and governance, providing efficient auditing policies during
      pipeline
      execution.

* **Kind Tags**: Every asset must include a `kinds` dictionary to quickly identify the underlying system or technology
  used for a given asset in the Dagster UI.

* **Group Name**: The `group_name` parameter in the asset decorator is mandatory. It must match the name of the model or
  ML task the asset belongs to. This field ensures assets are grouped appropriately in Dagster's UI and supports the
  organization of complex workflows by task or pipeline.

* **Return Type and Metadata**: All assets must return an `Output` object from Dagster with the type being returned. The
  Output must contains its metadata dictionary containing relevant runtime information. Metadata information will be
  used for creating assets object that can be used for different task.

* **Asset Description**: All assets must have asset description docstring which provides essential context for
  understanding, maintaining, and visualizing machine learning pipelines by documenting each asset’s purpose, origin,
  and logic.

Below is an example of an asset that follows the mandatory structure:

```python3
from dagster import asset, Output


@asset(tags={"domain": "ML", "pii": "false"},
       group_name="ml_task",
       kinds={"python"})
def asset_name() -> Output[Any]:
    """
    Asset description.
    """
    ...
    return Output(Any, metadata={...})
```

## ML Workflow Configuration

To ensure modularity and reproducibility in ML workflows, configuration parameters associated to each pipeline must
be defined using YAML files, one per pipeline. These configuration files must be stored in `ml_orchestrator/config`
directory. Each YAML file should begin with a top-level key that reflects the task it configures (e.g., `training`,
`reduction`). Below is an example configuration file with mandatory fields

```yaml title="ml_orchestrator/config/ml_workflow_params.yaml"
data:
  train_query: "train.sql"
  serving_query: "serving.sql"
  chunk_size: 1000
training:
  test_size: 0.3
  model_1:
    target_accuracy: 0.90
    scaler: "minmaxscaler"
    model_name: "histgradientboosting"
    params:
      learning_rate: 0.01
  model_2:
    target_accuracy: 0.90
    scaler: "minmaxscaler"
    model_name: "histgradientboosting"
    params:
      learning_rate: 0.01
serving:
  join_column:
    name: "join_column_name"
    type: "type"
  prediction_column:
    name: "column_predicted_name"
    type: "type"
  table_name: "table_name"
```

??? warning

    For better standardization, ensure this file includes the suffix `_params_`. Dagster will verify the presence of this 
    suffix in the file path before retrieving the ML configuration.

The `data` section defines the process for fetching data, using SQL queries for both, train and serving datasets. These
queries must run against the DuckDB resource. The `train_query` and `serving_query` fields in the data section refer
to the names of SQL files, which must be stored in the `queries` directory within the `ml_orchestrator` module. The
`training` section includes all configurations related to machine learning model training. This section can define
multiple models, each with its own scaler and algorithm-specific learning parameters. The purpose of using multiple
models is to enable a model agreement criterion during production inference. The `serving` section provides the
necessary information to convert model predictions stored in DuckDB tables for further use.

The corresponding path to each YAML file should be declared as a variable in the `assets/mlops_constants.py` file. It
is necessary only to specify the filename, not the full path.

```python3 title="ml_orchestrator/assets/mlops_constants.py"
workflow_example = "ml_workflow_params.yaml"
```

To access the configuration within an asset, use the `get_parameters` function located in the `utils/parameters`
directory. This function will automatically search for the file inside the `ml_orchestrator/config` directory. For
example, for an asset located in `ml_orchestrator/assets/mlops_data.py`

```python3 title="ml_orchestrator/assets/mlops_data.py"
from . import mlops_constants
from dagster import asset, Output
from utils.parameters import get_parameters


@asset(tags={"domain": "ML", "pii": "false"},
       group_name="ml_task",
       kinds={"python"})
def asset_name() -> Output[Any]:
    """
    Asset description.
    """
    config = get_parameters(filename=mlops_constants.workflow_example)
    ...
    return Output(Any, metadata={...})
```

??? warning

    The `get_parameters` function will raise an error if the top-level key in the YAML file does not include at least 
    `data` and `training` sections.

## Reusable Assets

In the majority of machine learning pipelines, certain tasks are performed repeatedly across multiple workflows. Writing
these assets separately for each pipeline can lead to code duplication. To address this, a reusable asset pattern is
adopted, where common logic is encapsulated inside an object-oriented wrapper. This approach allows a single
asset class to produce customized assets for different workflows, simply by adjusting the input parameters. The basic
structure of reusable asset object is shown as follows

```python3
from dagster import asset, Output


class ReusableAsset:
    def __init__(self,
                 input_parameter: Any,
                 asset_name: str,
                 group_name: str) -> None:
        self.input_parameter = input_parameter
        self.asset_name = asset_name
        self.group_name = group_name

    def create_asset(self):
        @asset(name=self.asset_name,
               tags={"domain": "ML", "pii": "false"},
               group_name=self.group_name,
               kinds={"python"})
        def _asset() -> Output[Any]:
            """
            Asset description.
            """
            variable = self.input_parameter
            ...
            return Output(Any, metadata={...})

        return _asset
```

Reusable asset concept brings several advantages. It avoids redundancy by allowing core logic to be implemented once and
reused across multiple workflows. This approach improves consistency, ensuring that all ML pipelines share the same
behavior for repeated tasks. It also simplifies maintenance, as any changes to the core logic are automatically
reflected in all pipelines and fully complaint with the [asset mandatory structure](#asset-mandatory-structure).

This repository provides a collection of reusable assets for each of the previously mentioned ML stages. These assets
are organized within `utils` directory, with subfolders created for each specific task. A detailed explanation of each
asset is provided below. It is important to mention, the assets leverage widely-used and well-established machine
learning techniques. For simplicity, these techniques are consolidated within the `artifacts` directory, specifically in
the `__init__.py` file. This file defines four dictionaries `scalers`, `reducers`, `regressors`, `classifiers` and `distribution`
containing different techniques according to each category. To use a specific technique, refer to its key name in the
configuration file.

## Assets for Data Fetching

The following assets are designed to facilitate data retrieval from DuckDB. There are two types of data fetching 
assets:

1. **Static data fetching** asset which executes a single, fixed SQL query to retrieve data.

2. **Dynamic data fetching** asset which uses a SQL template to generate queries dynamically, enabling retrieval 
of the same baseline data across multiple configurations (e.g., different geographic levels).

### Asset for Static Data Fetching

This asset is concerned helping to retrieve data directly from DuckDB with no effort. It is mandatory to properly set
up the DuckDB connection resource to avoid issues when using this asset. This asset is located in `data_fetcher`
directory.

#### Input Parameters

* **data_params** (str): Path to the YAML file containing the SQL file used for data fetching.
* **asset_name** (str): Name used to identify the asset for logging or visualization purposes.
* **group_name** (str): Name of the ML workflow group responsible for executing the data fetching process.
* **train** (bool): Flag indicating whether the query is for training (`True`) or serving (`False`).

#### Usage

Configuration parameters for data fetching must be defined within the ML configuration file. The top-level key
should be named `data`. Within this section, use the `train_query` or `serving_query` parameter to specify the name of
the SQL file containing the query, depending on the task as illustrated below.

```yaml title="ml_orchestrator/config/ml_workflow_params.yaml"
data:
  train_query: "train.sql"
  serving_query: "serving.sql"
  chunk_size: 10000
```

To create the data fetching asset, import `DucdDBDataFetcher` inside `ml_orchestrator/assets/mlops_data.py`

```python3 title="ml_orchestrator/assets/mlops_data.py"
from . import mlops_constants
from utils.data_fetcher import DucdDBDataFetcher

fetched_data = DucdDBDataFetcher(data_params=mlops_constants.workflow_example,
                                    asset_name="fetched_data",
                                    group_name="ml_task",
                                    train=True).create_asset()
```

Note that `data_params` variable was already defined in `mlops_constants.py` as explained
in [ML Worflow Configuration](#ml-workflow-configuration)

??? warning

    To prevent issues during model serving, this asset is configured to sort data alphabetically by column names. 
    Ensure that the column names are identical in both the training and serving queries.

#### Returned Metadata

* **n_samples**: The number of samples in the data asset.
* **n_features**: The number of features in tne data asset.

This asset is solely responsible for retrieving data from DuckDB. To prepare the data for model training,
including tasks such as data cleaning and target variable definition, a separate post-processing asset must
be implemented. An example of a post-process asset is as follows

```python3 title="ml_orchestrator/assets/mlops_transformation.py"
import pandas as pd
from utils.plot import plot_data
from dagster import asset, Output, AssetIn, MetadataValue


@asset(ins={"data": AssetIn("fetched_data")},
       tags={"domain": "ML", "pii": "false"},
       group_name="ml_task",
       kinds={"python"})
def process_data(data) -> Output[pd.DataFrame]:
    """
    Asset to process fetched data.
    """
    target_name = "target"
    data_training = data.drop(["column"], axis=1)
    data_training["target"] = pd.factorize(data_training["target"], sort=True)[0]

    figure = plot_data(data=data_training, target_name=target_name)

    return Output(data_training, metadata={"n_samples": MetadataValue.int(data_training.shape[0]),
                                           "n_features": MetadataValue.int(data_training.shape[1]),
                                           "target": MetadataValue.text(target_name),
                                           "data_plot": figure})
```

Note that the name of the target variable to be learned by the model is explicitly defined. The target variable can be 
specified either as a single string (for single-output models) or as a list of strings (for multi-output models), 
allowing flexibility depending on the prediction task. Dagster uses this metadata, using `AssetExecutionContext` object 
to inform downstream assets that this variable should be treated as the primary target for prediction. Additionally, 
the `plot_data` function, located in the `plot` directory, is used to generate visualization metadata for the dataset.

### Asset for Dynamic Data Fetching Based on a Template Query

This asset is designed to dynamically retrieve data from DuckDB using a parameterized template query. The template 
query allows the SQL logic to remain reusable and flexible, while execution-specific parameters (such as date ranges, 
partitions, or filtering criteria) are injected at runtime based on a configurable execution level. It is mandatory to 
properly configure the DuckDB connection resource to avoid issues when using this asset. This asset is located in 
the `data_fetcher` directory.

#### Input Parameters

* **data_params** (str): Path to the YAML file containing the SQL template and contextual parameters used for 
dynamic data fetching.
* **asset_name** (str): Name used to identify the asset for logging or visualization purposes.
* **partition_level** (PartitionsDefinition): Identifier of the **Dagster partition definition** associated with this asset. It specifies 
which context block under `data.levels` should be used to parameterize the SQL template at runtime.
* **group_name** (str): Name of the ML workflow group responsible for executing the data fetching process.

#### Usage

Configuration parameters for dynamic data fetching must be defined within the ML configuration file. The 
top-level key should be named `data`. This section controls how the SQL template is resolved and executed at runtime. 
Within the `data` section, specify:

* **query**: Name of the SQL file containing the Jinja2 template. The file must be located in the `queries` directory. 
The template will be rendered dynamically using the context defined for the selected execution level.
* **levels**: A mapping of execution level names (e.g., corresponding to Dagster partitions) to their 
respective template contexts. Each level defines the parameters that will be injected into the SQL template during 
execution.

A minimal configuration example is shown below:

```yaml title="ml_orchestrator/config/ml_workflow_params.yaml"
data:
  query: "dynamic_query.sql"
  levels:
    zip:
      context:
        tables:
          fico:
            name: int_zip_code_credit_time_series
            id_col: zcta_id
```

The corresponding `dynamic_query.sql` file might look like as follows

```sql title="ml_orchestrator/queries/dynamic_query.sql"
WITH fico_score_time_series AS (
    SELECT
        {{ tables.fico.id_col }},
        tnc_version_num,
        fico_score
    FROM mlops_prod.public.{{ tables.fico.name }}
)
SELECT *
FROM fico_score_time_series
```

Next, create the appropriate Dagster partitions. In `mlops_constants.py`, define the list of execution levels declared 
in the configuration file:

```python3 title="ml_orchestrator/assets/mlops_constants.py"

workflow_example = "ml_workflow_params.yaml"
template_levels = ["zip"]

```
Then in `ml_orchestrator/partitions/__init__.py`, create the Dagster partition definition:

```python3 title="ml_orchestrator/partitions/__init__.py"
from dagster import StaticPartitionsDefinition
from ..assets.mlops_constants import template_levels

template_partition = StaticPartitionsDefinition(template_levels)

```

To create the dynamic data fetching asset, import `DuckDBDynamicTemplateDataFetcher` inside 
`ml_orchestrator/assets/mlops_data.py` and attach the previously defined partition:

```python3 title="ml_orchestrator/assets/mlops_data.py"
from . import mlops_constants
from ..partitions import template_partition
from utils.data_fetcher import DuckDBDynamicTemplateDataFetcher

dynamic_data = DuckDBDynamicTemplateDataFetcher(data_params=mlops_constants.workflow_example,
                                            asset_name="template_data",
                                            partition_level= template_partition,
                                            group_name="ml_task").create_asset()
```
Note that the `data_params` variable was already defined in `mlops_constants.py` as explained 
in [ML Worflow Configuration](#ml-workflow-configuration). The purpose of the `partition_level` argument is to map each level defined in the 
configuration file to a corresponding Dagster partition key of the asset. In other words, every configured level 
becomes an independent partition, and the asset is materialized separately for each partition key.

This design ensures that data fetching is executed automatically on a per-level basis, with each partition rendering 
the SQL template using its associated context. As a result, the system enables fully automated and scalable data 
retrieval driven by Dagster partitioning and SQL template parameterization.

??? warning

    To prevent issues during model serving, this asset is configured to sort data alphabetically by column names. 
    Ensure that the column names are identical in both the training and serving queries.


#### Returned Metadata

* **n_samples**: The number of samples in the data asset.
* **n_features**: The number of features retained after dimensionality reduction.
* **col_key**: Column name for easy identification in downstream assets.

## Assets for Data Transformation

These assets are designed to assist with data transformation prior to model training. Available components include class
objects for reducing feature complexity and for splitting datasets into training and evaluation sets. These assets are
located in the `data_processing` directory.

### Train and Test Dataset Creation

The aim of this asset is to split data into random train and test subsets to support machine learning model training and
validation. In this particular case, the asset returns a `multi-asset` output, each representing either the training or
the test data subset.

#### Input Parameters

* **split_params** (str): Path to the YAML file containing the splitting configuration.
* **input_data_asset** (str): Name of the asset that provides the data to be splitted.
* **asset_name** (str): Name used to identify the asset for logging or visualization purposes.
* **train_asset_name** (str): Name used to identify the training data asset.
* **test_asset_name** (str): Name used to identify the test data asset.
* **model_partitions** (PartitionsDefinition): Dagster Partition definition containing information of ensemble of models
* **group_name** (str): Name of the ML workflow group where the splitting data is applied.
* **figure** (bool) = Boolean flag to generate plot in asset metadata. Default value is `True`.

#### Usage

Configuration parameters for data splitting must be defined within the ML configuration file. The top-level key
should be named `training`, with the test split ratio specified under the `test_size` field, as illustrated below. 

```yaml title="ml_orchestrator/config/ml_workflow_params.yaml"
training:
  test_size: 0.3
  model_1:
    target_accuracy: 0.90
    scaler: "minmaxscaler"
    model_name: "mlp"
    params:
      hidden_layer_sizes: [ 500, 500, 300 ]
```
Sub-level model string patterns are used to create an ensemble of models. Instead of training a single model, a set 
of models is trained so that predictions are based on agreement across the ensemble. In this approach, the training 
and test dataset creation must be executed for each model in the ensemble, as controlled by the `model_partitions` 
variable. The first step to materialize the training and test data assets is to create the Dagster partition associated 
with the ensemble of models.

In `ml_orchestrator/partitions/__init__.py`, use the `static_partition_from_parameters` function to automatically create 
the ensemble partition based on the configuration file. This function is located in the `utils/partitions` subfolder.

```python3 title="ml_orchestrator/partitions/__init__.py"
from . import mlops_constants
from utils.partitions import static_partition_from_parameters

workflow_example_partitions = static_partition_from_parameters(model_params=mlops_constants.workflow_example,
                                                        model_asset_name="workflow_example")
```

This function automatically determines how many models are defined under the `training` section of the configuration file, 
following the specified `model` string pattern. It then creates a static partition that is used to materialize the 
training and test dataset assets for each model in the ensemble. The `model_asset_name parameter is a string used to 
distinguish the partition associated with each machine learning pipeline.

To create the train and test data assets, import `TrainTestDataAsset` object inside
`ml_orchestrator/assets/mlops_transformation.py`

```python3 title="ml_orchestrator/assets/mlops_transformation.py"
from . import mlops_constants
from ..partitions import workflow_example_partitions
from utils.data_processing import TrainTestDataAsset

splitter_asset = TrainTestDataAsset(split_params=mlops_constants.workflow_example,
                                    input_data_asset="process_data",
                                    asset_name="splitter_asset",
                                    train_asset_name="train_asset",
                                    test_asset_name="test_asset",
                                    model_partitions= workflow_example_partitions,
                                    group_name="ml_task").create_asset()
```

Data split is controlled by `test_size` parameter.

#### Returned Metadata

Returned metadata is the same for train and test data assets.

* **subset**: Indicates the portion of the data asset represented. It can be "train" and "test".
* **size**: Represents the proportion of the original data asset allocated in each split data asset.
* **model_partition**: String key associated to each model of the ensemble.
* **n_samples**: The number of samples in the split data asset.
* **n_features**: The number of features in the split data asset.
* **target**: The name (names) of the target variable used for model training.
* **data_plot**: A visual representation of the split data.

## Assets for Model Training

These assets are designed to support one of the most critical tasks in machine learning workflows, the model training.
They are located in `utils/training` directory.

### Classification and Regression

This repository provides reusable assets for both classification and regression tasks. The classification training asset
is implemented as `ClassifierModelTraining`, while the regression counterpart is `RegressorModelTraining`. Despite the
difference in class names, both assets share the same input parameters for consistency and ease of use. Both 
classification and regression support single and multi output variant for model training.

#### Input Parameters

* **training_params** (str): Path to the YAML file containing training configuration.
* **training_data_asset** (str): Name of the asset that provides the training data.
* **asset_name** (str): Name used to identify the asset for logging or visualization purposes.
* **model_partitions** (PartitionsDefinition): Dagster Partition definition containing information of ensemble of models
* **group_name** (str): Name of the ML workflow group where the splitting data is applied.
* **multioutput** (bool) = Boolean flag to train either single or multi output models. Default value is `False`.

#### Usage

Training configuration parameters must be specified in the YAML file. The top-level key should be named `training`
followed by the model option, with the desired scaler and model type defined under the `scaler` and `model_name` fields,
respectively, as shown below. Hyperparameters specific to the selected model must be defined under the `params` section.

```yaml title="ml_orchestrator/config/ml_workflow_params.yaml"
training:
  test_size: 0.3
  model_1:
    target_accuracy: 0.90
    scaler: "minmaxscaler"
    model_name: "histgradientboosting"
    params:
      learning_rate: 0.01
```

??? warning

    Currently, the ML orchestrator does not include hyperparameter optimization. Therefore, it is important to ensure 
    that the parameters specified are well-tuned and appropriate for effective model training. 

Strings for `scaler` and `model_name` fields must match keys in the `scalers` and `classifiers` or `regressors`
dictionaries (depending on the task), respectively, located in the `artifacts` directory. To create the training asset,
import the object in `ml_orchestrator/assets/mlops_training.py`. In the example below it is shown the asset creation
for both classification and regression.

In `ml_orchestrator/partitions/__init__.py`, use the `static_partition_from_parameters` function to automatically create 
the ensemble partition based on the configuration file. This function is located in the `utils/partitions` subfolder.

```python3 title="ml_orchestrator/partitions/__init__.py"
from . import mlops_constants
from utils.partitions import static_partition_from_parameters

workflow_example_partitions = static_partition_from_parameters(model_params=mlops_constants.workflow_example,
                                                        model_asset_name="workflow_example")
```

This function automatically determines how many models are defined under the `training` section of the configuration file, 
following the specified `model` string pattern. It then creates a static partition that is used to materialize the 
training and test dataset assets for each model in the ensemble. The `model_asset_name parameter is a string used to 
distinguish the partition associated with each machine learning pipeline.

```python3 title="ml_orchestrator/assets/mlops_training.py"
from . import mlops_constants
from ..partitions import workflow_example_partitions
from utils.training import ClassifierModelTraining, RegressorModelTraining

classifier_model = ClassifierModelTraining(training_params=mlops_constants.workflow_example,
                                           training_data_asset="train_asset",
                                           asset_name="classifier_model",
                                           model_partitions=workflow_example_partitions,
                                           group_name="ml_task",
                                           multioutput=False).create_asset()

regressor_model = RegressorModelTraining(training_params=mlops_constants.workflow_example,
                                         training_data_asset="train_asset",
                                         asset_name="regressor_model",
                                         model_partitions=workflow_example_partitions,
                                         group_name="ml_task",
                                         multioutput=False).create_asset()
```

It is important to note that both the `ClassifierModelTraining` and `RegressorModelTraining` objects return
a [Pipeline](https://scikit-learn.org/stable/modules/generated/sklearn.pipeline.Pipeline.html)
asset, meaning a sequence of data transformer, determined by the `scaler` field, followed by a final predictor,
specified by `model_name` field by each model option.

#### Returned Metadata

The returned metadata is the same for both classification and regression training.

* **scaler**: The name of the preprocessing scaler applied to the input features before training.
* **model**: The name of the model trained on the processed data.
* **task**: A fixed string indicating the type of task performed. It can be "classification"  or "regression".
* **model_partition**: String key associated to each model of the ensemble.
* **multioutput**: Boolean flag to see what kind of model output is returned by the model.


### Model Quality Evaluation

This asset is committed to evaluate how well the model generalizes to data that was not used for training.

#### Input Parameters

* **model_asset** (str): Name of the assets containing the trained model.
* **data_input_asset** (str): Name of the asset containing the unsplit data. This data is used to plot the model’s
  decision function, when applicable.
* **test_data_asset** (str): Name of the asset containing the test dataset. This data is used to compute the model's
  accuracy score.
* **asset_name** (str): Name used to identify the asset for logging or visualization purposes.
* **model_partitions** (PartitionsDefinition): Dagster Partition definition containing information of ensemble of models.
* **group_name** (str): Name of the ML workflow group where the model evaluation is going to be evaluated.
* **figure** (bool) = Boolean flag to generate plot in asset metadata. Default value is `True`.

#### Usage

In `ml_orchestrator/partitions/__init__.py`, use the `static_partition_from_parameters` function to automatically create 
the ensemble partition based on the configuration file. This function is located in the `utils/partitions` subfolder.

```python3 title="ml_orchestrator/partitions/__init__.py"
from . import mlops_constants
from utils.partitions import static_partition_from_parameters

workflow_example_partitions = static_partition_from_parameters(model_params=mlops_constants.workflow_example,
                                                        model_asset_name="workflow_example")
```

To create the model evaluation asset, import `EvaluateQualityTraining` object in
`ml_orchestrator/assets/mlops_training.py`. This asset work
in the same way no matter if the model is for classification or regression

```python3 title="ml_orchestrator/assets/mlops_training.py"
from utils.training import EvaluateQualityTraining
from ..partitions import workflow_example_partitions

model_quality = EvaluateQualityTraining(model_asset="model_asset",
                                        data_input_asset="process_data",
                                        test_data_asset="test_data",
                                        asset_name="model_quality",
                                        model_partitions=workflow_example_partitions,
                                        group_name="ml_task").create_asset()
```

#### Returned Metadata

* **model_accuracy**: Model's accuracy score value.
* **model_plot**: Decision function plot for classification or real-predicted plot comparison for regression models.
* **model_partition**: String key associated to each model of the ensemble.
* **task**: A fixed string indicating the type of task performed. It can be "classification"  or "regression".
* **target**: The name (names) of the target variable used for model training.


## Assets for Model Deployment


These assets are designed to use trained models for performing prediction tasks and writing the results to a DuckDB table.

### Asset for Model Serving

This asset is responsible for generating prediction values from trained models on serving data. It supports handling multiple prediction requests from a set of models, with the final prediction computed through a model agreement strategy.


#### Input Parameters


* **serving_params** &#40;str&#41;: Path to the YAML file containing the serving configuration.

* **input_data_asset** &#40;str&#41;: Data asset containing the serving data to be used for prediction.

* **models_assets** &#40;list[str]&#41;: List of models to be used for generating predictions.

* **asset_name** &#40;str&#41;: Name used to identify the asset for logging or visualization purposes.

* **group_name** &#40;str&#41;: Name of the ML workflow group where the model evaluation is going to be evaluated.


#### Usage
The configuration parameters for model serving should follow the structure below. By default, the top-level key must be named `serving`. Mandatory fields are `join_column` which specifies the name of the column used to perform the SQL join with tables in the datamart and `prediction_column` which defines the name of the column where the predicted values will be stored. Both fields must include the column name and its corresponding data type.


```yaml title="ml_orchestrator/config/ml_workflow_params.yaml"

serving:
  join_column:
    name: "join_column_name"
    type: "type"

  prediction_column:
    name: "column_predicted_name"
    type: "type"

  table_name: "table_name"

```


To create the model serving asset, import `ModelServing` object in `ml_orchestrator/assets/mlops_deployment.py`. The `models_assets` parameter is a list containing the names of all model assets that will be used for prediction. These names must match the corresponding asset names used during model training. The asset internally handles the model agreement process to compute the final prediction values.

```python3 title="ml_orchestrator/assets/mlops_deployment.py"

from . import mlops_constants

from utils.serving import ModelServing


models_assets = ["model_1", "model_2"]

model_serving = ModelServing(serving_params=mlops_constants.workflow_example,
                             input_data_asset="serving_data",
                             models_assets=models_assets,
                             asset_name="model_serving",
                            group_name="ml_task").create_asset();

```

#### Returned Metadata


* **n_samples**: Number of samples in which prediciton was carried out.

* **prediction_column**: Name of the column in whihc predicted values are grouped.


## Asset for Model Persistence

This asset handles local model persistence for use in the model registry and subsequent deployment. It is located in the
`persistence` directory. Notably, the asset automatically creates a new version of the model each time the ML workflow
pipeline is executed. All models are saved in the `models` directory for organized storage and access.

#### Input Parameters

* **model_params** (str): Path to the YAML file containing training configuration.
* **model_asset** (str): Name of the assets containing the trained model.
* **model_quality_asset** (str): Name of the asset who evaluated quality of trained model.
* **asset_name** (str): Name used to identify the asset for logging or visualization purposes.
* **model_partitions** (PartitionsDefinition): Dagster Partition definition containing information of ensemble of models.
* **group_name** (str): Name of the ML workflow group where the model evaluation is going to be evaluated.

#### Usage

In `ml_orchestrator/partitions/__init__.py`, use the `static_partition_from_parameters` function to automatically create 
the ensemble partition based on the configuration file. This function is located in the `utils/partitions` subfolder.

```python3 title="ml_orchestrator/partitions/__init__.py"
from . import mlops_constants
from utils.partitions import static_partition_from_parameters

workflow_example_partitions = static_partition_from_parameters(model_params=mlops_constants.workflow_example,
                                                        model_asset_name="workflow_example")
```

To create the model local persistence asset, import `SaveMLArtifact` object in
`ml_orchestrator/assets/mlops_registry.py`.

```python3 title="ml_orchestrator/assets/mlops_registry.py"
from . import mlops_constants
from utils.persistence import SaveMLArtifact
from ..partitions import workflow_example_partitions

model_artifact = SaveMLArtifact(model_params=mlops_constants.workflow_example,
                                model_asset="model_asset",
                                model_quality_asset="model_quality",
                                asset_name="model_artifact",
                                model_partitions=workflow_example_partitions,
                                group_name="ml_task", ).create_asset()
```

??? warning

    `SaveMLArtifact` object only will persist the trained model if its accuracy score value is greater or equal 
    to `target_accuracy`.

#### Returned Metadata

* **artifact_path**: Path to location in which model artifact was saved.
* **model_partitions** (PartitionsDefinition): Dagster Partition definition containing information of ensemble of models.
* **model_version**: Current version for the model.
* **persisted**: Boolean flag to know if model were persisted.
* **task**: A fixed string indicating the type of task performed. It can be "classification"  or "regression".
* **target**: The name (names) of the target variable used for model training.