from sqlalchemy.engine import CursorResult
from profuturo.common import define_extraction, register_time, notify, truncate_table
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark, get_bigquery_pool,configure_mitedocta_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms, _get_spark_session, _create_spark_dataframe
from pyspark.sql.functions import concat, col, row_number, lit, lpad
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pyspark.sql.window import Window
from sqlalchemy import text
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
from io import BytesIO
from PIL import Image
from profuturo.env import load_env
import requests
import time
import json
import sys
import jwt
import os

load_env()
postgres_pool = get_postgres_pool()
bigquery_pool = get_bigquery_pool()
storage_client = storage.Client()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])
term =int(sys.argv[2])
print(phase,term, user,area )
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
    except Exception as error:
        print("ERROR:", error)
        return -1


def get_headers():
    non_repudiation_token = get_token()
    if non_repudiation_token != -1:
        return {"Authorization": f"Bearer {non_repudiation_token}"}
    else:
        return {}

def upload_to_gcs(row):
    id_value = row["id"]
    bytea_data = row["fto_imagen"]

    # Convertir bytes a imagen
    image = Image.open(BytesIO(bytea_data))

    # Guardar imagen localmente (opcional)
    # image.save(f"local/{id_value}.png")

    # Subir imagen a GCS
    blob_name = f"{prefix}/{id_value}.png"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Convertir imagen a bytes antes de subirla
    byte_stream = BytesIO()
    image.save(byte_stream, format="PNG")
    byte_stream.seek(0)

    blob.upload_from_file(byte_stream, content_type="image/png")

def delete_all_objects(bucket_name, prefix):
    # Crea una instancia del cliente de Cloud Storage
    storage_client = storage.Client()

    # Obtiene el bucket
    bucket = storage_client.bucket(bucket_name)

    # Lista todos los objetos en el bucket con el prefijo especificado
    blobs = bucket.list_blobs(prefix=prefix)

    # Elimina cada objeto
    for blob in blobs:
        #print(f"Eliminando objeto: {blob.name}")
        blob.delete()

def get_blob_info(bucket_name, prefix):
    # Crea una instancia del cliente de Cloud Storage
    storage_client = storage.Client()

    # Obtiene el bucket
    bucket = storage_client.bucket(bucket_name)

    # Lista todos los objetos en el bucket con el prefijo especificado
    blobs = bucket.list_blobs(prefix=prefix)

    # Lista para almacenar información de blobs
    blob_info_list = []

    # Recorre todos los blobs y obtiene información
    for blob in blobs:
        # Divide el nombre del blob en partes usando '-'
        parts = blob.name.split('-')

        # Asegúrate de que haya al menos tres partes en el nombre
        if len(parts) == 4:
            # Obtiene la información de id, formato y área
            blob_info = {
                "FTC_POSICION_PDF": parts[0].split('/')[1],
                "FCN_ID_FORMATO_EDOCTA": int(parts[1]),
                "FCN_ID_AREA": int(parts[2].split('.')[0]),
                "FTC_URL_IMAGEN": f"https://storage.cloud.google.com/{bucket_name}/{blob.name}",
                "FTC_IMAGEN": f"{blob.name}",
                "FTC_SIEFORE": parts[3].split('.')[0] if parts[3].split('.')[0] != 'sinsiefore' else None
            }

            blob_info_list.append(blob_info)

        if len(parts) > 4:
            # Obtiene la información de id, formato y área
            blob_info = {
                "FTC_POSICION_PDF": parts[0].split('/')[1],
                "FCN_ID_FORMATO_EDOCTA": int(parts[1]),
                "FCN_ID_AREA": int(parts[2].split('.')[0]),
                "FTC_URL_IMAGEN": f"https://storage.cloud.google.com/{bucket_name}/{blob.name}",
                "FTC_IMAGEN": f"{blob.name}",
                "FTC_SIEFORE": f"{parts[3]}-{parts[4].split('.')[0]}"
            }

            blob_info_list.append(blob_info)

    return blob_info_list

with define_extraction(phase, area, postgres_pool, bigquery_pool) as (postgres, bigquery):
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

        delete_all_objects(bucket_name, prefix)
        delete_all_objects(bucket_name, 'profuturo-archivos-dummy')

        query = """
                SELECT
                DISTINCT
                concat("FTC_CODIGO_POSICION_PDF",'-',tcie."FCN_ID_FORMATO_ESTADO_CUENTA",'-', ra."FCN_ID_AREA",'-',COALESCE(tcie."FTC_DESCRIPCION_SIEFORE",'sinsiefore')) AS ID,"FTO_IMAGEN" AS FTO_IMAGEN
                FROM "GESTOR"."TTGESPRO_CONFIG_IMAGEN_EDOCTA" tcie
                INNER JOIN "GESTOR"."TTGESPRO_ROL_USUARIO" ru ON CAST(tcie."FTC_USUARIO" AS INT) = ru."FCN_ID_USUARIO"
                INNER JOIN "GESTOR"."TCGESPRO_ROL_AREA" ra ON ru."FCN_ID_ROL" =  ra."FCN_ID_ROL"
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
            StructField("FTC_SIEFORE", StringType(), True)
        ])

        df = spark.createDataFrame(blob_info_list, schema=schema)

        _write_spark_dataframe(df, configure_postgres_spark, '"ESTADO_CUENTA"."TTEDOCTA_IMAGEN_DUMMY"')

        headers = get_headers()  # Get the headers using the get_headers() function

        for i in range(10):
            response = requests.get(url_reca, headers= headers)
            print(response)
            # Verifica si la petición fue exitosa
            if response.status_code == 200:
                # Si la petición fue exitosa, puedes acceder al contenido de la respuesta de la siguiente manera:
                content = response.content.decode('utf-8')
                data = json.loads(content)
                if data['data']['statusText'] == 'finalizado':
                    break
                time.sleep(8)
            else:
                # Si la petición no fue exitosa, puedes imprimir el código de estado para obtener más información
                print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")
                break

        for i in range(10):
            response = requests.get(url_ret, headers= headers)
            print(response)
            # Verifica si la petición fue exitosa
            if response.status_code == 200:
                # Si la petición fue exitosa, puedes acceder al contenido de la respuesta de la siguiente manera:
                content = response.content.decode('utf-8')
                data = json.loads(content)
                if data['data']['statusText'] == 'finalizado':
                    break
                time.sleep(8)
            else:
                # Si la petición no fue exitosa, puedes imprimir el código de estado para obtener más información
                print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")
                break
