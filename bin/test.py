from sqlalchemy.engine import CursorResult
from profuturo.common import define_extraction, register_time, notify, truncate_table
from profuturo.database import get_postgres_pool, get_postgres_oci_pool, configure_postgres_spark, configure_postgres_oci_spark, get_bigquery_pool,configure_mitedocta_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms, _get_spark_session, _create_spark_dataframe, extract_dataset_spark
from pyspark.sql.functions import concat, col, row_number, lit, lpad
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pyspark.sql.window import Window
from google.cloud import storage, bigquery

from io import BytesIO
from PIL import Image
from sqlalchemy import text
from profuturo.env import load_env

import pandas as pd
import requests
import random
import string
import time
import json
import sys
import jwt
import os


load_env()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()
storage_client = storage.Client()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])
bucket_name = os.getenv("BUCKET_ID")
print(bucket_name)
prefix =f"{os.getenv('PREFIX_BLOB')}"
print(prefix)
url = os.getenv("URL_MUESTRAS_RECA")
print(url)


################### OBTENCIÓN DE MUESTRAS #####################################



with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 100)
    spark.conf.set("spark.default.parallelism", 100)

    with register_time(postgres_pool, phase, term_id, user, area):
        # Extracción de tablas temporales
        query_temp = """
        SELECT
        "FTN_ID_TIPO_SUBCTA", "FCN_ID_REGIMEN", "FCN_ID_CAT_SUBCTA", "FCC_VALOR", "FTC_TIPO_CLIENTE"
        FROM "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"
        """

        extract_dataset_spark(
            configure_postgres_spark,
            configure_postgres_oci_spark,
            query_temp,
            '"MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"'
        )

        # Extracción de tablas temporales
        query_temp = """
        SELECT
        "FTN_ID_SIEFORE", "FTC_DESCRIPCION", "FTC_DESCRIPCION_CORTA", "FTC_SIEFORE"
        FROM "MAESTROS"."TCDATMAE_SIEFORE"
        """
        extract_dataset_spark(
            configure_postgres_spark,
            configure_postgres_oci_spark,
            query_temp,
            '"MAESTROS"."TCDATMAE_SIEFORE"'
        )

        # Extracción de tablas temporales
        query_temp = """
        SELECT
        "FTN_ID_MOV_PROFUTURO_CONSAR", "FCN_ID_MOVIMIENTO_CONSAR","FCN_ID_MOVIMIENTO_PROFUTURO", "FCN_MONPES"
        FROM "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR"
        """
        extract_dataset_spark(
            configure_postgres_spark,
            configure_postgres_oci_spark,
            query_temp,
            '"MAESTROS"."TTGESPRO_MOV_PROFUTURO_CONSAR"'
        )

        # Extracción de tablas temporales
        query_temp = """
        SELECT
        "FTN_ID_MOVIMIENTO_CONSAR", "FTC_DESCRIPCION", "FTC_MOV_TIPO_AHORRO",
        "FTB_INTEGRACION_DIAS_COTIZADOS_SALARIO_BASE", "FCN_ID_REFERENCIA"
        FROM "MAESTROS"."TCDATMAE_MOVIMIENTO_CONSAR"
        """
        extract_dataset_spark(
            configure_postgres_spark,
            configure_postgres_oci_spark,
            query_temp,
            '"MAESTROS"."TCDATMAE_MOVIMIENTO_CONSAR"'
        )

        # Extracción de tablas temporales
        query_temp = """
        SELECT "FTN_ID_PERIODO", "FTC_PERIODO" FROM "GESTOR"."TCGESPRO_PERIODO"
        """

        extract_dataset_spark(
            configure_postgres_spark,
            configure_postgres_oci_spark,
            query_temp,
            '"MAESTROS"."TCGESPRO_PERIODO"'
        )
