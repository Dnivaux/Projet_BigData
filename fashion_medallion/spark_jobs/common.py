import argparse
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200"))
        .getOrCreate()
    )


def setup_logger(log_file: str, name: str) -> logging.Logger:
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


def parse_ingestion_date(raw_date: str | None) -> datetime:
    if raw_date:
        return datetime.strptime(raw_date, "%Y-%m-%d")
    return datetime.utcnow()


def add_common_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--ingestion-date", required=False, help="Date d'ingestion au format YYYY-MM-DD")
    parser.add_argument("--log-file", required=True, help="Chemin du fichier de log .txt")
    return parser
