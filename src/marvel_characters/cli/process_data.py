from __future__ import annotations

import argparse

import pandas as pd
import yaml
from loguru import logger
from pyspark.sql import SparkSession

from marvel_characters.config import ProjectConfig
from marvel_characters.data_processor import DataProcessor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root_path",
        action="store",
        default=None,
        type=str,
        required=True,
    )
    parser.add_argument(
        "--env",
        action="store",
        default=None,
        type=str,
        required=True,
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    config_path = f"{args.root_path}/files/project_config_marvel.yml"
    config = ProjectConfig.from_yaml(config_path=config_path, env=args.env)

    logger.info("Configuration loaded:")
    logger.info(yaml.dump(config, default_flow_style=False))

    spark = SparkSession.builder.getOrCreate()
    filepath = f"{args.root_path}/files/data/marvel_characters_dataset.csv"

    df = pd.read_csv(filepath)
    logger.info("Marvel data loaded for processing.")

    data_processor = DataProcessor(df, config, spark)
    data_processor.preprocess()

    X_train, X_test = data_processor.split_data()
    logger.info("Training set shape: %s", X_train.shape)
    logger.info("Test set shape: %s", X_test.shape)

    logger.info("Saving data to catalog")
    data_processor.save_to_catalog(X_train, X_test)


if __name__ == "__main__":
    main()
