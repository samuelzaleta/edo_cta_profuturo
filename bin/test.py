from sqlalchemy.engine import CursorResult
from profuturo.common import define_extraction, register_time, notify, truncate_table
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark, get_bigquery_pool,configure_mitedocta_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms, _get_spark_session, _create_spark_dataframe
from pyspark.sql.functions import concat, col, row_number, lit, lpad
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pyspark.sql.window import Window
from sqlalchemy import text
from google.cloud import storage, bigquery
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from PIL import Image
from profuturo.env import load_env
import pandas as pd
import requests
import sys
import random
import string
import time
import os
import json

load_env()
postgres_pool = get_postgres_pool()
bigquery_pool = get_bigquery_pool()
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



################### OBTENCIÓN DE MUESTRAS #######################################
def find_samples(samples_cursor: CursorResult):
    samples = set()
    i = 0
    for batch in samples_cursor.partitions(50):
        for record in batch:
            account = record[0]
            consars = record[1]
            i += 1

            for consar in consars:
                if consar not in configurations:
                    continue

                configurations[consar] = configurations[consar] - 1

                if configurations[consar] == 0:
                    del configurations[consar]

                samples.add(account)

                if len(configurations) == 0:
                    print("Cantidad de registros: " + str(i))
                    return samples

    result = tuple(list(configurations.keys()))
    print(result)
    try:
        descripcion_consar = postgres.execute(text("""
            SELECT "FTC_DESCRIPCION" FROM "MAESTROS"."TCDATMAE_MOVIMIENTO_CONSAR"
            WHERE "FTN_ID_MOVIMIENTO_CONSAR" IN :consar
            """), {'consar': result}).fetchall()

        print(descripcion_consar)
        # Creating a DataFrame
        df = pd.DataFrame(descripcion_consar, columns=['Concepto Consar'])  # Replace 'Column_Name' with an appropriate column name
        html_table = df.to_html()

        notify(
            postgres,
            'No se encontraron suficientes registros para los Conceptos CONSAR',
            phase,
            area,
            term_id,
            message=f"No se encontraron suficientes muestras para los Conceptos CONSAR:",
            details=html_table,
            aprobar=False,
            descarga=False,
            reproceso=False,
        )

        return samples
    except:
        return 'sin muestras'


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

def move_blob(source_bucket, destination_bucket, source_blob_name, destination_blob_name):
    source_blob = source_bucket.blob(source_blob_name)
    destination_blob = destination_bucket.blob(destination_blob_name)

    # Copiar el blob del bucket fuente al bucket de destino
    destination_blob.rewrite(source_blob)

def move_files_parallel(source_bucket_name, destination_bucket_name, source_prefix="", destination_prefix="", num_threads=10):
    # Inicializa los clientes de almacenamiento
    source_client = storage.Client()
    destination_client = storage.Client()
    print("\n",source_bucket_name)
    print(destination_bucket_name)
    # Obtén los buckets
    source_bucket = source_client.get_bucket(source_bucket_name)
    destination_bucket = destination_client.get_bucket(destination_bucket_name)

    # Lista todos los archivos en el bucket fuente con el prefijo dado
    blobs = source_bucket.list_blobs(prefix=source_prefix)

    # Usa ThreadPoolExecutor para ejecutar la copia de blobs en paralelo
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []

        for blob in blobs:
            # Crear el nombre del blob de destino con el prefijo de destino
            destination_blob_name = destination_prefix + blob.name[len(source_prefix):]
            futures.append(executor.submit(move_blob, source_bucket, destination_bucket, blob.name, destination_blob_name))

        # Espera a que todos los hilos hayan completado
        for future in futures:
            future.result()

    print("Movimiento de archivos completado")



with define_extraction(phase, area, postgres_pool, bigquery_pool) as (postgres, bigquery):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 100)
    spark.conf.set("spark.default.parallelism", 100)

    with register_time(postgres_pool, phase, term_id, user, area):

        ###########################  IMAGENES   #################################################
        truncate_table(postgres, 'TTEDOCTA_IMAGEN')

        delete_all_objects(bucket_name, prefix)

        delete_all_objects(bucket_name, 'profuturo-archivos')

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

        _write_spark_dataframe(df, configure_postgres_spark, '"ESTADO_CUENTA"."TTEDOCTA_IMAGEN"')








