The Machine Learning orchestrator, implemented in Dagster, is designed to follow established MLOps best practices.
This ensures that machine learning pipelines beginning in data ingestion to model deployment are scalable, reproducible,
observable, and explainable.

Dagster was chosen for its powerful support of modular data assets, which enable us to break down the ML lifecycle into
reusable components. Each stage of the pipeline treated as a distinct, versioned asset, making the workflow easy to
manage and maintain. In addition, Dagster’s orchestration capabilities allow us to track exactly what happened at each
step, validate that everything runs as expected and debug workflows with logging and metadata. A Typical MLOps
workflow is shown in Figure 1.

<figure>
  <img src="../figures/ml_workflow.svg" alt="ML Workflow" width="900"/>
  <figcaption>Figure 1. Typical machine learning workflow.</figcaption>
</figure>

Figure 1 illustrates a standard machine learning workflow in which each rectangle represents a specific stage of the
process. To replicate the workflow, an asset-based approach in which each step of the ML lifecycle is modeled as a
distinct Dagster asset. For better organization and maintainability, these assets are grouped by their corresponding ML
workflow stages. The main directory for implementing these assets is `ml_orchestrator/assets`. This folder is structured
to mirror the ML workflow depicted in Figure 1, with a dedicated Python file for each step. The files and their
responsibilities are as follows:

* **mlops_data.py**: Handles raw data ingestion.
* **mlops_transformation.py**: Contains all data transformation assets, including feature engineering and train/test
  dataset creation.
* **mlops_training.py**: Defines the model training pipeline and includes model evaluation logic.
* **mlops_registry.py**: Manages model persistence and registration.
* **mlops_deployment.py**: Includes assets related to model serving, specifically to create tables containing prediction values which are stored in Snowflake.
* **mlops_monitoring**: Includes assets for serving and model monitoring puporses.
* **mlops_constants.py**: Stores configuration details for each model to be implemented.

Figure 2 shows dedicated file in which assets must be develop for each step of the ML workflow.

<figure>
  <img src="../figures/ml_workflow_dagster.svg" alt="ML Workflow Dagster" width="900"/>
  <figcaption>Figure 2. Dedicated file for asset development.</figcaption>
</figure>

It is important to note that a separate ML workflow must be implemented for each machine learning task.
Dagster will distinguishes between these workflows using the `group_name` parameter in each asset. In addition, specific
configuration and parameters by ML workflow must be defined in `mlops_constants.py` file. Further details on this
structure and the mandatory format of each asset is provided in the [ML Asset Development](ml_asset_development.md)
section.
 
