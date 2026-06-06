from dagster_duckdb import DuckDBResource

duckdb_resource = {"duckdb": DuckDBResource(database="data/local.duckdb")}
