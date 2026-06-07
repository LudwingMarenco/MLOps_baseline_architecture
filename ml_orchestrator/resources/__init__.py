import os
import subprocess

from dagster import ConfigurableResource
from dagster_duckdb import DuckDBResource

duckdb_resource = {"duckdb": DuckDBResource(database="data/local.duckdb")}


class LocalStorageResource(ConfigurableResource):
    base_path: str = "models"
    repo_path: str = "."
    gto_enabled: bool = True

    def save(self, artifact_name: str, buffer) -> str:
        os.makedirs(self.base_path, exist_ok=True)
        path = os.path.join(self.base_path, artifact_name)
        with open(path, "wb") as f:
            f.write(buffer.getvalue())
        return path

    def register(self, model_name: str) -> None:
        if self.gto_enabled:
            result = subprocess.run(
                ["gto", "register", model_name, "--repo", self.repo_path],
                capture_output=True,
                text=True,
                # no check=True here
            )
            if result.returncode != 0 and "already registered" not in result.stderr:
                raise subprocess.CalledProcessError(
                    result.returncode, result.args, result.stdout, result.stderr
                )

    def promote(self, model_name: str, stage: str = "dev") -> None:
        if self.gto_enabled:
            result = subprocess.run(
                [
                    "gto",
                    "assign",
                    model_name,
                    "--stage",
                    stage,
                    "--repo",
                    self.repo_path,
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                if "already in stage" in result.stderr:
                    return
                raise subprocess.CalledProcessError(
                    result.returncode, result.args, result.stdout, result.stderr
                )


local_storage_resource = {"model_persistor": LocalStorageResource(base_path="models")}
