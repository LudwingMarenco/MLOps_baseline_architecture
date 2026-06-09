import fcntl
import os
import subprocess

import joblib
from dagster import ConfigurableResource
from dagster_duckdb import DuckDBResource
from filelock import FileLock

duckdb_resource = {"duckdb": DuckDBResource(database="data/local.duckdb")}


class LocalStorageResource(ConfigurableResource):
    base_path: str = "models"

    def save(self, artifact_name: str, buffer) -> str:
        os.makedirs(self.base_path, exist_ok=True)
        path = os.path.join(self.base_path, artifact_name)
        with open(path, "wb") as f:
            f.write(buffer.getvalue())
        return path

    def _version_path(self, model_name: str) -> str:
        return os.path.join(self.base_path, f"{model_name}_version.txt")

    def _read_version(self, model_name: str) -> tuple:
        path = self._version_path(model_name)
        if not os.path.exists(path):
            return (0, 0, 0)
        with open(path, "r") as f:
            parts = f.read().strip().lstrip("v").split(".")
            return tuple(int(p) for p in parts)

    def _write_version(self, model_name: str, version: tuple) -> str:
        version_str = f"v{version[0]}.{version[1]}.{version[2]}"
        with open(self._version_path(model_name), "w") as f:
            f.write(version_str)
        return version_str

    def register(self, model_name: str) -> str:
        lock_path = os.path.join(self.base_path, ".version_lock")
        with FileLock(lock_path, timeout=120):
            major, minor, patch = self._read_version(model_name)
            new_version = (major, minor, patch + 1)
            return self._write_version(model_name, new_version)

    def get_version(self, model_name: str) -> str:
        major, minor, patch = self._read_version(model_name)
        return f"v{major}.{minor}.{patch}"

    def load(self, artifact_path: str):
        with open(artifact_path, "rb") as f:
            return joblib.load(f)


local_storage_resource = {"model_persistor": LocalStorageResource(base_path="models")}
