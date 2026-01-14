from __future__ import annotations

import argparse

from loguru import logger
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from marvel_characters.config import ProjectConfig
from marvel_characters.serving.model_serving import ModelServing


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

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    model_version = dbutils.jobs.taskValues.get(taskKey="train_model", key="model_version")

    config = ProjectConfig.from_yaml(config_path=config_path, env=args.env)
    logger.info("Loaded config file.")

    catalog_name = config.catalog_name
    schema_name = config.schema_name
    endpoint_name = "marvel-character-model-serving"

    model_serving = ModelServing(
        model_name=f"{catalog_name}.{schema_name}.marvel_character_model_custom",
        endpoint_name=endpoint_name,
    )

    model_serving.deploy_or_update_serving_endpoint(version=model_version)
    logger.info("Started deployment/update of the Marvel serving endpoint.")


if __name__ == "__main__":
    main()
