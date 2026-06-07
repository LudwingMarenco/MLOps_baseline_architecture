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


class DuckDBDynamicChunkedFetcher:
    def __init__(
        self,
        data_params: str,
        op_name: str,
    ) -> None:
        self.data_params = data_params
        self.op_name = op_name

    def create_op(self):
        @op(
            name=self.op_name,
            tags={"domain": "ML", "pii": "false"},
            out=DynamicOut(pd.DataFrame),
        )
        def _op(context: OpExecutionContext, duckdb: ResourceParam[DuckDBResource]):
            parameters = get_parameters(self.data_params)
            level = context.partition_key
            sql_file = parameters["data"]["serving_query"]
            sql_path = os.path.join("ml_orchestrator/queries/", sql_file)

            with open(sql_path, "r") as file:
                sql_template = file.read()

            template = Template(sql_template)
            values = parameters["data"]["levels"][level]["context"]
            query = template.render(**values)

            with duckdb.get_connection() as conn:
                result = conn.execute(query)
                columns = [desc[0] for desc in result.description]
                chunk_id = 0
                run = True

                while run:
                    data = result.fetchmany(parameters["data"]["chunk_size"])
                    if not data:
                        run = False
                    else:
                        data_chunked = pd.DataFrame(data, columns=columns)
                        data_chunked = data_chunked[sorted(data_chunked.columns)]
                        context.log.info(
                            f"Fetched chunk {chunk_id} with {len(data_chunked)} rows"
                        )
                        yield DynamicOutput(
                            value=data_chunked,
                            mapping_key=f"level_{level}_chunk_{chunk_id:05d}",
                        )
                        chunk_id += 1

        return _op
