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

## Running the orchestrator

## Development

A comprehensive guide for developing ML workflows with Dagster is available in the `docs` folder.
To view the documentation locally, run:

```bash
  mkdocs serve
 ```

Then open http://127.0.0.1:8000 in your browser to navigate through the content. 