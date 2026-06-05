"""
Ingestão de dados CSV para camada RAW via Spark Connect.

Padrão idempotente: utiliza write.mode("overwrite") com saveAsTable,
garantindo que re-execuções produzem o mesmo resultado sem perda de dados.
"""

import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SPARK_URL = os.getenv("SPARK_CONNECT_URL", "sc://spark-connect:15002")
RAW_SCHEMA = "raw_vault"
CSV_PATH = "/opt/spark/work-dir/data/ecommerce_customer_analytics.csv"


def get_spark() -> SparkSession:
    return SparkSession.builder.remote(SPARK_URL).getOrCreate()


def ingest_customers(spark: SparkSession) -> None:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(CSV_PATH)

    # Metadados de ingestão (Data Vault pattern)
    df = df.withColumn("load_datetime", current_timestamp()).withColumn(
        "record_source", lit("ecommerce_customer_analytics.csv")
    )

    # Cria database no Hive Metastore (idempotente)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {RAW_SCHEMA}")

    # Overwrite atômico — não faz DROP antes, evita janela de perda
    table_name = f"{RAW_SCHEMA}.raw_customers"
    df.write.mode("overwrite").option(
        "path", f"s3a://warehouse/{RAW_SCHEMA}.db/raw_customers"
    ).saveAsTable(table_name)

    count = spark.sql(f"SELECT count(*) as total FROM {table_name}").collect()[0]["total"]
    logger.info("✓ Tabela %s criada com %d registros", table_name, count)


if __name__ == "__main__":
    try:
        spark = get_spark()
        ingest_customers(spark)
        spark.stop()
    except Exception as e:
        logger.error("Falha na ingestão: %s", e)
        sys.exit(1)
