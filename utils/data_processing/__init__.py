import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetOut,
    MetadataValue,
    Output,
    PartitionsDefinition,
    multi_asset,
)
from sklearn.model_selection import train_test_split

from utils.parameters import (
    get_asset_metadata,
    get_parameters,
)


class TrainTestDataAsset:
    def __init__(
        self,
        split_params: str,
        input_data_asset: str,
        asset_name: str,
        train_asset_name: str,
        test_asset_name: str,
        model_partitions: PartitionsDefinition,
        group_name: str,
        figure: bool = True,
    ) -> None:
        self.split_params = split_params
        self.input_data_asset = input_data_asset
        self.asset_name = asset_name
        self.train_asset_name = train_asset_name
        self.test_asset_name = test_asset_name
        self.model_partitions = model_partitions
        self.group_name = group_name
        self.figure = figure

    def create_asset(self):
        @multi_asset(
            name=self.asset_name,
            ins={"data": AssetIn(self.input_data_asset)},
            partitions_def=self.model_partitions,
            outs={
                self.train_asset_name: AssetOut(
                    tags={"domain": "ML", "pii": "false"},
                    kinds={"python", "scikitlearn"},
                ),
                self.test_asset_name: AssetOut(
                    tags={"domain": "ML", "pii": "false"},
                    kinds={"python", "scikitlearn"},
                ),
            },
            deps=[self.input_data_asset],
            group_name=self.group_name,
        )
        def _asset(
            context: AssetExecutionContext, data
        ) -> tuple[Output[pd.DataFrame], Output[pd.DataFrame]]:
            """
            Asset to split data into random train and test subsets.
            """

            job_key = context.partition_key
            metadata = get_asset_metadata(context, asset_name=self.input_data_asset)
            parameters = get_parameters(self.split_params)
            target_name = metadata["target"]
            test_size = parameters["training"]["test_size"]

            features = data.drop(columns=target_name)
            target = data[target_name]
            x_train, x_test, y_train, y_test = train_test_split(
                features, target, stratify=target, test_size=test_size, shuffle=True
            )
            training_data = x_train.copy()
            training_data[target_name] = y_train
            test_data = x_test.copy()
            test_data[target_name] = y_test

            train_figure = None
            test_figure = None
            if isinstance(target_name, list):
                metadata_text = MetadataValue.json(target_name)
            else:
                metadata_text = MetadataValue.text(target_name)

            return (
                Output(
                    training_data,
                    metadata={
                        "subset": MetadataValue.text("train"),
                        "size": MetadataValue.float(1 - test_size),
                        "model_partition": MetadataValue.text(job_key),
                        "n_samples": MetadataValue.int(x_train.shape[0]),
                        "n_features": MetadataValue.int(x_train.shape[1]),
                        "target": metadata_text,
                        "data_plot": train_figure,
                    },
                ),
                Output(
                    test_data,
                    metadata={
                        "subset": MetadataValue.text("test"),
                        "size": MetadataValue.float(test_size),
                        "model_partition": MetadataValue.text(job_key),
                        "n_samples": (x_test.shape[0]),
                        "n_features": MetadataValue.int(x_test.shape[1]),
                        "target": metadata_text,
                        "data_plot": test_figure,
                    },
                ),
            )

        return _asset
