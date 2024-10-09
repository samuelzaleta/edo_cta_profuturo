from sqlalchemy.engine import CursorResult
from profuturo.common import define_extraction, register_time, notify, truncate_table
from profuturo.database import get_postgres_pool, configure_postgres_spark, get_postgres_oci_pool
from profuturo.extraction import _write_spark_dataframe, extract_terms, _get_spark_session, _create_spark_dataframe
from profuturo.imagen import upload_to_gcs, delete_all_objects, get_blob_info
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
from profuturo.env import load_env
import requests
import time
import json
import sys
import jwt
import os

load_env()
postgres_pool = get_postgres_pool()
storage_client = storage.Client()
postgres_oci_pool = get_postgres_oci_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])
term =int(sys.argv[2])
bucket_name = os.getenv("BUCKET_ID")
print(bucket_name)
prefix =f"imagenesDummy"
print(prefix)
url_reca = os.getenv("URL_DUMMY_RECA")
url_ret = os.getenv("URL_DUMMY_RET")
print(url_reca)
print(url_ret)



def get_token():
    try:
        payload = {"isNonRepudiation": True}
        secret = os.environ.get("JWT_SECRET")  # Ensure you have set the JWT_SECRET environment variable

        if secret is None:
            raise ValueError("JWT_SECRET environment variable is not set")

        # Set expiration time 10 seconds from now
        expiration_time = datetime.utcnow() + timedelta(seconds=10)
        payload['exp'] = expiration_time.timestamp()  # Setting expiration time directly in payload

        # Create the token
        non_repudiation_token = jwt.encode(payload, secret, algorithm='HS256')

        return non_repudiation_token
    except Exception:
        return -1


def get_headers():
    non_repudiation_token = get_token()
    if non_repudiation_token != -1:
        return {"Authorization": f"Bearer {non_repudiation_token}"}
    else:
        return {}


with define_extraction(phase, area, postgres_pool, ) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()
    spark.conf.set("spark.sql.files.maxPartitionBytes", "256m")
    spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 20)

    with register_time(postgres_pool, phase, term_id, user, area):
        ###########################  IMAGENES   #################################################
        truncate_table(postgres, 'TTEDOCTA_IMAGEN_DUMMY')

        try:
            delete_all_objects(bucket_name, prefix)
        except:
            pass

        query = """
        SELECT
        DISTINCT
        concat("FTC_CODIGO_POSICION_PDF",'-',tcie."FCN_ID_FORMATO_ESTADO_CUENTA",'-', tcie."FCN_ID_AREA",'-',COALESCE(tcie."FTC_RANGO_EDAD", 'SinRangoEdad'), '-', COALESCE(tcie."FTC_DESCRIPCION_SIEFORE",'sinsiefore') ) AS ID,"FTO_IMAGEN" AS FTO_IMAGEN
        FROM "GESTOR"."TTGESPRO_CONFIG_IMAGEN_EDOCTA" tcie
        """

        imagenes_df = _create_spark_dataframe(spark, configure_postgres_spark, query,params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        imagenes_df.show()

        imagenes_df.foreach(upload_to_gcs)

        # Obtiene la información del blob
        blob_info_list = get_blob_info(bucket_name, prefix)

        schema = StructType([
            StructField("FTC_POSICION_PDF", StringType(), True),
            StructField("FCN_ID_FORMATO_EDOCTA", IntegerType(), True),
            StructField("FCN_ID_AREA", IntegerType(), True),
            StructField("FTC_URL_IMAGEN", StringType(), True),
            StructField("FTC_IMAGEN", StringType(), True),
            StructField("FTC_SIEFORE", StringType(), True),
            StructField("FTC_RANGO_EDAD", StringType(), True)
        ])

        df = spark.createDataFrame(blob_info_list, schema=schema)

        _write_spark_dataframe(df, configure_postgres_spark, '"ESTADO_CUENTA"."TTEDOCTA_IMAGEN_DUMMY"')


        for i in range(10):
            headers = get_headers()  # Get the headers using the get_headers() function
            response = requests.get(url_reca, headers=headers)  # Pass headers with the request
            print(response)

            if response.status_code == 200:
                content = response.content.decode('utf-8')
                data = json.loads(content)
                if data['data']['statusText'] == 'finalizado':
                    break
                time.sleep(20)
            else:
                print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")
                break

        for i in range(10):
            headers = get_headers()  # Get the headers using the get_headers() function
            response = requests.get(url_ret, headers=headers)  # Pass headers with the request
            print(response)

            if response.status_code == 200:
                content = response.content.decode('utf-8')
                data = json.loads(content)
                if data['data']['statusText'] == 'finalizado':
                    break
                time.sleep(20)
            else:
                print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")
                break


