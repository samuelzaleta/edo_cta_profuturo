from profuturo.database import get_postgres_pool, get_postgres_oci_pool
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from io import BytesIO
from PIL import Image
from profuturo.env import load_env
import sys
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


def upload_to_gcs(row):
    id_value = row["id"]
    bytea_data = row["fto_imagen"]

    # Convertir bytes a imagen
    image = Image.open(BytesIO(bytea_data))

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
        print(parts, len(parts))

        if parts[3] =='' and parts[4] != "":
            # Obtiene la información de id, formato y área
            blob_info = {
                "FTC_POSICION_PDF": parts[0].split('/')[1],
                "FCN_ID_FORMATO_EDOCTA": int(parts[1]),
                "FCN_ID_AREA": int(parts[2]),
                "FTC_URL_IMAGEN": f"https://storage.cloud.google.com/{bucket_name}/{blob.name}",
                "FTC_IMAGEN": f"{blob.name}",
                "FTC_RANGO_EDAD":'',
                "FTC_SIEFORE": parts[4].split('.')[0]
            }
            blob_info_list.append(blob_info)

            # Asegúrate de que haya al menos tres partes en el nombre
        if len(parts)==6 and parts[3] !='':
            # Obtiene la información de id, formato y área
            blob_info = {
                "FTC_POSICION_PDF": parts[0].split('/')[1],
                "FCN_ID_FORMATO_EDOCTA": int(parts[1]),
                "FCN_ID_AREA": int(parts[2].split('.')[0]),
                "FTC_URL_IMAGEN": f"https://storage.cloud.google.com/{bucket_name}/{blob.name}",
                "FTC_IMAGEN": f"{blob.name}",
                "FTC_RANGO_EDAD": f"{parts[3]}-{parts[4]}",
                "FTC_SIEFORE": parts[5].split('.')[0] if parts[5].split('.')[0] != 'sinsiefore' else None
            }
            blob_info_list.append(blob_info)

        # Asegúrate de que haya al menos tres partes en el nombre
        if len(parts) == 6 and parts[3] =='':
            # Obtiene la información de id, formato y área
            blob_info = {
                "FTC_POSICION_PDF": parts[0].split('/')[1],
                "FCN_ID_FORMATO_EDOCTA": int(parts[1]),
                "FCN_ID_AREA": int(parts[2].split('.')[0]),
                "FTC_URL_IMAGEN": f"https://storage.cloud.google.com/{bucket_name}/{blob.name}",
                "FTC_IMAGEN": f"{blob.name}",
                "FTC_SIEFORE": f"{parts[4]}-{parts[5].split('.')[0]}"
            }
            blob_info_list.append(blob_info)

        # Asegúrate de que haya al menos tres partes en el nombre
        if len(parts) == 7:
            # Obtiene la información de id, formato y área
            blob_info = {
                "FTC_POSICION_PDF": parts[0].split('/')[1],
                "FCN_ID_FORMATO_EDOCTA": int(parts[1]),
                "FCN_ID_AREA": int(parts[2].split('.')[0]),
                "FTC_URL_IMAGEN": f"https://storage.cloud.google.com/{bucket_name}/{blob.name}",
                "FTC_IMAGEN": f"{blob.name}",
                "FTC_RANGO_EDAD": f"{parts[3]}-{parts[4]}",
                "FTC_SIEFORE": f"{parts[5]}-{parts[6].split('.')[0]}"
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
