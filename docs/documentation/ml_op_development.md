The majority of machine learning pipelines can be effectively implemented using an asset-based approach in Dagster. This
is because Dagster is good at managing and tracking the entire machine learning workflow, from data ingestion and
training to model persistence.

However, when it comes to serving models on large datasets (e.g., with more than one million records), assets may not be
the best option due to potential out-of-memory issues. In such cases, the recommended approach is to process the data in
chunks. Dagster supports this pattern through the use of operations (`op`), which allow for scalable, memory-efficient
processing. This section explains how to implement ops functions and introduces a set of pre-built, reusable ops
objects tailored for efficient model serving.

## Operations Mandatory Structure

To maintain traceability and reusability across all machine learning workflows, every operation must adhere to a
standardized structure, regardless of its internal logic or assigned group. The key components of this standard are
outlined below:

* **Asset Tags**: Each operation must include a `tags` dictionary in its decorator with the following required fields:

    * **"domain"**: Specifies the relevant domain. For machine learning operations, the default value is `"ML"`.
    * **"pii"**: A flag indicating whether the asset contains personally identifiable information (`"true"` or
      `"false"`).

* **Input Definition**: Each operation must have a `ins` dictionary specifying the expected input names and their types.

* **Output Type**: All assets must return an `Out` object, clearly defining the type of data being returned.

* **Operation Description**: Every operation must include a descriptive docstring. This provides essential context about
  the operation’s purpose, logic, and origin, aiding in understanding and maintenance.

Below is an example of an operation that follows the mandatory structure:

```python3
from dagster import op, In, DynamicOut, DynamicOutput


@op(ins={"data": In(Any)},
    out=DynamicOut(Any),
    tags={"domain": "ML", "pii": "false"})
def op_name(data: Any) -> Any:
    """
    Operation description.
    """
    ...
    map_key = AnyStr

    yield DynamicOutput(Any, mapping_key=map_key)
```

Note that example above uses `yield` statement. This is necessary when the operation needs to painless process the data
in portions or chunks. The chunk size and partitioning logic can be defined inside the function. However, for an
operation function which process each portion use a simple `return` statement as shown below:

```python3
from dagster import op, In, Out,


@op(ins={"data": In(Any)},
    out=Out(Any),
    tags={"domain": "ML", "pii": "false"})
def op_name(data: Any) -> Any:
    """
    Operation description.
    """
    ...

    return Any
```

## Running Operations

Operations are executed within a `graph object, where each step is performed sequentially or mapped over dynamic
outputs.

```python3
from dagster import graph


@graph
def graph_name():
    chunked_data = chunk_data_op()  # Dynamically outputs multiple chunks
    processed_chunk = chunked_data.map(process_chunk)  # Processes each chunk individually
```

The advantage of defining a graph is that it is explicitly specified the connections and dependencies between
operations. These relationships are clearly visualized in the Dagster UI. To execute the graph, it must first be
converted into a job. This is done using Dagster’s `to_job` method, as shown below:

```python3

job_name = graph_name.to_job()
```
The last step is to register the job inside `Definitions` object of Dagster

```python3
from dagster import Definitions

all_jobs = [job_name]
defs = Definitions(
    assets=...,
    jobs=all_jobs,
    schedules=...,
    sensors=...,
    resources=...,
)
```

## Reusable Operations

The reusable operation pattern adopted, where common logic is encapsulated within an object-oriented wrapper allows a
single operation class to generate customized operations for different workflows simply by adjusting input parameters.
The basic structure of a reusable operation object is shown below:

```python3
from dagster import op, In, Out


class ReusableOperation:
    def __init__(self,
                 input_parameter: Any,
                 op_name: str

                 ) -> None:
        self.input_parameter = input_parameter

    self.op_name = op_name


def create_op(self):
    @op(name=self.op_name,
        ins={"data": In(Any)},
        tags={"domain": "ML", "pii": "false"},
        out=Out(Any))
    def _op(data) -> Any:
        """
        Operation description.
        """
        variable = self.input_parameter
        ...
        return Any

    return _op
```

## Operation for Chunked Data Fetching

This operation fetches data in chunks from Snowflake Resource.

#### Input Parameters

* **data_params** (str): Path to the YAML file containing the SQL file used for data serving.
* **op_name** (str): Name used to identify the operation for logging or visualization purposes.
* **train** (bool): Flag indicating whether the query is for training (`True`) or serving (`False`).

#### Usage

Configuration parameters for chunked data fetching must be defined within the ML configuration file. The top-level key
should be named `data`. Within this section, use the `train_query` or `serving_query`parameter to specify the name of
the SQL file containing the query, depending on the task. The size of each data chunk is defined using the `chunk_size`
parameter, as illustrated below.

```yaml title="ml_orchestrator/config/ml_workflow_params.yaml"
data:
  train_query: "train.sql"
  serving_query: "serving.sql"
  chunk_size: 10000
```

To create the chunked data fetching operation, import `SnowflakeChunkedFetcher` inside
`ml_orchestrator/assets/mlops_data.py`

```python3 title="ml_orchestrator/assets/mlops_data.py"
from . import mlops_constants
from utils.data_fetcher import SnowflakeChunkedFetcher

chunked_fetched_data = SnowflakeChunkedFetcher(data_params=mlops_constants.workflow_example,
                                               op_name="chunked_fetched_data",
                                               train=False).create_op()
```

Note that `data_params` variable was already defined in `mlops_constants.py` as explained
in [ML Worflow Configuration](ml_asset_development#ml-workflow-configuration). Use the `op`
within a `graph` object, depending on the specific task that the unit of computation is intended to perform.

??? warning

    To prevent issues during model serving, this operation is configured to sort data alphabetically by column names. 
    Ensure that the column names are identical in both the training and serving queries.

## Operations for Model Deployment

These operations are designed to use trained models for performing prediction tasks and writing the results to a
Snowflake table.

### Operation for Model Serving

This operation is responsible for generating prediction values from trained models on serving data. It supports
handling multiple prediction requests from a set of models, with the final prediction computed through a model agreement
strategy.

#### Input Parameters

* **serving_params** (str): Path to the YAML file containing the SQL file used for data fetching.
* **artifact_model_asset** (str): Name of model artifact asset to be used for generating predictions.
* **accuracy_model_asset** (str): Name of asset which evaluates quality of training.
* **model_partitions** (PartitionsDefinition): Dagster Partition definition containing information of ensemble of models.
* **op_name** (str): Name used to identify the operation for logging or visualization purposes.

#### Usage

In `ml_orchestrator/partitions/__init__.py`, use the `static_partition_from_parameters` function to automatically create 
the ensemble partition based on the configuration file. This function is located in the `utils/partitions` subfolder.

```python3 title="ml_orchestrator/partitions/__init__.py"
from . import mlops_constants
from utils.partitions import static_partition_from_parameters

workflow_example_partitions = static_partition_from_parameters(model_params=mlops_constants.workflow_example,
                                                        model_asset_name="workflow_example")
```

The configuration parameters for model serving should follow the structure below. By default, the top-level
key must be named `serving`. Mandatory fields are `join_column` which specifies the name of the column used to perform
the SQL join with tables in the datamart and `prediction_column` which defines the name of the column where the
predicted values will be stored. Both fields must include the column name and its corresponding data type.

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

To create the model serving operation, import `ModelChunkedServing` object in
`ml_orchestrator/assets/mlops_deployment.py`. The `models_assets` parameter is a list containing the names of all model
assets that will be used for prediction. These names must match the corresponding asset names used during model
training. The asset internally handles the model agreement process to compute the final prediction values.

```python3 title="ml_orchestrator/assets/mlops_deployment.py"
from . import mlops_constants
from utils.serving import ModelChunkedServing
from ..partitions import workflow_example_partitions


model_serving = ModelChunkedServing(serving_params=mlops_constants.workflow_example,
                                    artifact_model_asset= "model_asset",
                                    accuracy_model_asset= "model_quality",
                                    model_partitions=workflow_example_partitions,
                                    op_name="model_serving").create_op()
```

Model serving is performed on each chunk of data individually. Use the `op` to apply the model to each chunk in
`ml_orchestrator/assets/mlops_deployment.py`by creating a `graph` object

```python3 title="ml_orchestrator/assets/mlops_deployment.py"
from dagster import graph
from . import mlops_constants
from utils.serving import ModelChunkedServing
from ..partitions import workflow_example_partitions
from ..assets.mlops_data import chunked_fetched_data

model_serving = ModelChunkedServing(serving_params=mlops_constants.workflow_example,
                                    artifact_model_asset= "model_asset",
                                    accuracy_model_asset= "model_quality",
                                    model_partitions=workflow_example_partitions,
                                    op_name="model_serving").create_op()


@graph
def model_serving_graph():
    chunks = chunked_fetched_data()
    predicted_chunks = chunks.map(model_serving)
```

Then, define the job in `ml_orchestrator/jobs/__init__.py`

```python3 title="ml_orchestrator/jobs/__init__.py"
from ..assets.mlops_deployment import model_serving_graph

model_serving_graph_job = model_serving_graph.to_job(name="model_serving_graph_job",
                                                     description="Model Serving")
```