import json
import os

import pandas as pd
from dagster import (
    AssetExecutionContext,
    DynamicOut,
    DynamicOutput,
    MetadataValue,
    OpExecutionContext,
    Output,
    PartitionsDefinition,
    ResourceParam,
    asset,
    op,
)
from dagster_duckdb import DuckDBResource
from jinja2 import Template

from utils.parameters import get_parameters


class DuckDBDataFetcher:
    def __init__(
        self, data_params: str, asset_name: str, group_name: str, train: bool = True
    ) -> None:
        self.data_params = data_params
        self.asset_name = asset_name
        self.group_name = group_name
        self.train = train

    def create_asset(self):
        @asset(
            name=self.asset_name,
            tags={"domain": "ML", "pii": "false"},
            group_name=self.group_name,
            kinds={"python", "duckdb"},
        )
        def _asset(
            context: AssetExecutionContext,
            duckdb: ResourceParam[DuckDBResource],
        ) -> Output[pd.DataFrame]:
            """
            Asset to fetch data from DuckDB.
            """
            parameters = get_parameters(self.data_params)
            sql_file = parameters["data"][
                "train_query" if self.train else "serving_query"
            ]
            sql_path = os.path.join("ml_orchestrator/queries/", sql_file)

            with open(sql_path, "r") as file:
                query = file.read()

            with duckdb.get_connection() as conn:
                if self.train:
                    data = conn.execute(query).df()
                else:
                    data = conn.execute(query).fetchmany(
                        parameters["serving"]["chunk_size"]
                    )
                    data = pd.DataFrame(
                        data, columns=[desc[0] for desc in conn.description]
                    )

            if data.empty:
                raise ValueError("No data returned from the query.")

            data = data[sorted(data.columns)]
            context.log.info(f"Fetched {len(data)} rows")
            return Output(
                data,
                metadata={
                    "n_samples": MetadataValue.int(data.shape[0]),
                    "n_features": MetadataValue.int(data.shape[1]),
                },
            )

        return _asset
