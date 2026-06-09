# Dagster Machine Learning Operation Orchestrator

An end-to-end machine learning system built around established MLOps best practices, ensuring that every stage of the ML lifecycle, from data ingestion to model deployment, is scalable, reproducible, observable, and explainable.

Dagster was chosen for its powerful support of modular data assets, enabling us to decompose the ML lifecycle into reusable, independently versioned components. Each pipeline stage is treated as a distinct asset, making the workflow straightforward to manage, extend, and maintain. Dagster's orchestration capabilities further allow us to track exactly what happened at each step, validate that everything runs as expected, and debug workflows through comprehensive logging and metadata.

## Getting Started

1. **Set Up Virtual Environment**: Create a virtual environment by running:

    ```bash
    ./scripts/mlops_create_venv.sh
    ```

2. **Activate Virtual Environment**: Activate your virtual environment by running:

    ```bash
     source scripts/mlops_activate_venv.sh
    ```
3. **Set Up Dagster**: MLOps Dagster Orchestrator will be inside [ml_orchestrator](ml_orchestrator)
   code location. Code location is installed as a package in editable mode by running:
    ```bash
     ./scripts/mlops_install_code_location.sh
    ```
4. **Run Dagster**: To start the Dagster UI web server run
    ```bash
     ./scripts/mlops_start_orchestrator.sh 
    ```
   Open http://127.0.0.1:35695 in your browser to see the ml-orchestrator.

## Running the Orchestrator

1. In the Dagster UI, go to **Assets** in the upper menu, then click **View Lineage**. Hover over the `churn_modeling_workflow_one` box and right-click to trigger materialization by clicking in **Materialize assets (7)**. Dagster will prompt you to select a partition, choose **All** on the right panel and click **Launch Backfill**. This is the only materialization that needs to be triggered manually. Once complete, the orchestrator will automatically simulate the full end-to-end lifecycle of the ML workflow.

2. Go to the **Runs** menu at the top of the UI to monitor the materialization status. Once the
status shows **Success**, navigate to **Automation** and activate the sensors in the following
order:

   a. `data_client_one_retraining_sensor`: Triggers model retraining based on monitoring status
   and model version. It activates when the status is **RED** and the current model version is
   being used for serving. This sensor checks for these conditions every 15 minutes.

   b. `data_client_one_serving_sensor`: Triggers the serving pipeline once all ensemble models
   have been successfully persisted for a defined version. This sensor performs its validation
   check every 10 minutes.

   c. `data_client_one_monitoring_sensor`: Materializes the monitoring pipeline to evaluate
   serving data quality, feature drift, prediction drift, and model performance. The status is
   computed every 3 minutes.

   d. `data_client_one_training_sensor`: Triggers model training when accuracy constraints are
   not met for successful model registration.

## Development

A comprehensive guide for developing ML workflows with Dagster is available in the `docs` folder.
To view the documentation locally, run:

```bash
  mkdocs serve
 ```

Then open http://127.0.0.1:8000 in your browser to navigate through the content. 