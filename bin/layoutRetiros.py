from profuturo.extraction import  extract_terms, _get_spark_session, _create_spark_dataframe
from profuturo.database import get_postgres_pool,get_postgres_oci_pool, configure_postgres_oci_spark, get_bigquery_pool
from pyspark.sql.functions import concat, col, row_number, lit, lpad, udf,date_format, rpad, translate
from pyspark.sql.functions import monotonically_increasing_id
from profuturo.common import define_extraction, register_time, notify
from google.cloud import storage, bigquery
from pyspark.sql.types import DecimalType
from profuturo.env import load_env
from datetime import datetime, timedelta
import requests
import time
import json
import sys
import jwt
import math
import os



load_env()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()
storage_client = storage.Client()
phase = int(sys.argv[1])
term = int(sys.argv[2])
user = int(sys.argv[3])
area = int(sys.argv[4])
print(phase,term, user,area)
bucket_name = os.getenv("BUCKET_ID")
print(bucket_name)
prefix =f"{os.getenv('PREFIX_BLOB')}"
#url = os.getenv("URL_CORRESPONDENSIA_RET")
#print(url)
correo = os.getenv("CORREO_CORRESPONDENCIA")
print(correo)
decimal_pesos = DecimalType(16, 2)



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

def get_buckets():
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        if bucket.name.startswith("edo_cuenta_profuturo"):
            return bucket.name

bucket = storage_client.get_bucket(get_buckets())

def str_to_gcs(data, name):
    blob = bucket.blob(f"correspondenciaRet/{name}")
    blob.upload_from_string(data.encode("iso_8859_1"), content_type="text/plain")
    print(F"SE CREO EL ARCHIVO DE correspondencia/{name}")

def send_email( to_address, subject, body):

    message = "{}\n\n{}".format(subject, body)
    datos = {
        'to': to_address,
        'subject': subject,
        'text': message
    }
    response = requests.post(url, data=datos,  verify=False)

    if response.status_code == 200:
        # Si la petición fue exitosa, puedes acceder al contenido de la respuesta de la siguiente manera:
        print("La solicitud fue exitosa")
    else:
        # Si la petición no fue exitosa, puedes imprimir el código de estado para obtener más información
        print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")


def process_dataframe(df, identifier):
    return df.rdd.flatMap(lambda row: [([identifier] + [row[i] for i in range(len(row))])]).collect()


with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 100)
    spark.conf.set("spark.default.parallelism", 100)

    with register_time(postgres_pool, phase, term_id, user, area):
        body_message = ""
        query_retiro = """
        SELECT
        "FCN_ID_EDOCTA", "FCN_NUMERO_CUENTA", "FCN_ID_PERIODO",
        "FTN_SDO_INI_AHO_RET","FTN_SDO_INI_AHO_VIV", "FTN_SDO_TRA_AHO_RET",
        "FTN_SDO_TRA_AHO_VIV", "FTN_SDO_REM_AHO_RET","FTN_SDO_REM_AHO_VIV",
        "FTC_LEY_PENSION", "FTC_REGIMEN", "FTC_SEGURO", "FTC_TIPO_PENSION",
        "FTC_FON_ENTIDAD", "FTC_FON_NUMERO_POLIZA", "FTN_FON_MONTO_TRANSF",
        to_char("FTD_FON_FECHA_TRANSF"::timestamp, 'yyyymmdd') as "FTD_FON_FECHA_TRANSF",
        "FTN_FON_RETENCION_ISR", "FTC_AFO_ENTIDAD",
        "FTC_AFO_MEDIO_PAGO", "FTC_AFO_RECURSOS_ENTREGA",
        to_char("FTD_AFO_FECHA_ENTREGA"::timestamp, 'yyyymmdd') as  "FTD_AFO_FECHA_ENTREGA",
        "FTC_AFO_RETENCION_ISR", 
        ' ' AS "FTC_INFNVT_ENTIDAD",
        '0' AS "FTC_INFNVT_CTA_BANCARIA",
        '0' AS "FTC_INFNVT_RECURSOS_ENTREGA", 
        ' ' AS "FTC_INFNVT_FECHA_ENTREGA",
        '0' AS "FTC_INFNVT_RETENCION_ISR",
        "FTN_PENSION_INSTITUTO_SEG",
        "FTN_SALDO_FINAL", "FTN_TIPO_TRAMITE", "FTC_ARCHIVO",
        CASE
            WHEN "FTC_FECHA_EMISION" = '10101' THEN '00010101'
            ELSE to_char(to_date(SUBSTRING("FTC_FECHA_EMISION", 1, 8), 'YYYYMMDD'), 'yyyymmdd')
        END AS "FTC_FECHA_EMISION",
        CASE
            WHEN "FTC_FECHA_INICIO_PENSION" = '10101' THEN '00010101'
            ELSE to_char(to_date(SUBSTRING("FTC_FECHA_INICIO_PENSION", 1, 8), 'YYYYMMDD'), 'yyyymmdd')
        END AS "FTC_FECHA_INICIO_PENSION",
        to_char("FTD_FECHA_LIQUIDACION"::timestamp, 'yyyymmdd') AS  "FTD_FECHA_LIQUIDACION",
        "FTD_FECHAHORA_ALTA",
        "FTC_USUARIO_ALTA"
        FROM "ESTADO_CUENTA"."TTEDOCTA_RETIRO"
        WHERE "FCN_ID_PERIODO" = :term
        """

        query_general_retiro = """
        SELECT 
        "FCN_NUMERO_CUENTA", "FCN_ID_PERIODO", "FTC_NOMBRE",
        "FTC_CALLE_NUMERO", "FTC_COLONIA","FTC_MUNICIPIO", 
        "FTC_CP", "FTC_ENTIDAD", "FTC_CURP", "FTC_RFC", 
        "FTC_NSS", "FCN_ID_EDOCTA"
        FROM "ESTADO_CUENTA"."TTEDOCTA_RETIRO_GENERAL"
        WHERE "FCN_ID_PERIODO" = :term
        """

        retiros_general_df = _create_spark_dataframe(spark, configure_postgres_oci_spark, query_general_retiro,
                                             params={"term": term_id, "start": start_month, "end": end_month,
                                                     "user": str(user)})

        retiros_general_df = retiros_general_df.withColumn("FCN_NUMERO_CUENTA", lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0"))


        retiros_df = _create_spark_dataframe(spark, configure_postgres_oci_spark, query_retiro,
                                                     params={"term": term_id,
                                                             "user": str(user)})
        retiros_df = retiros_df.fillna("").fillna(" ")
        print(retiros_df.count())

        retiros_df = retiros_df.withColumn("FCN_NUMERO_CUENTA",
                                                           lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0"))

        df = (
            retiros_general_df.join(retiros_df, 'FCN_NUMERO_CUENTA').select(
                col("FTD_FECHA_LIQUIDACION").alias("FTC_FECHA_EMISION_1"), "FTC_NOMBRE", "FTC_CALLE_NUMERO", "FTC_COLONIA",
                "FTC_MUNICIPIO", "FTC_CP","FTC_ENTIDAD", "FTC_CURP", "FTC_RFC",
                "FTC_NSS",col("FTN_SDO_INI_AHO_RET").cast(decimal_pesos),
                col("FTN_SDO_INI_AHO_VIV").cast(decimal_pesos),
                col("FTN_SDO_TRA_AHO_RET").cast(decimal_pesos),
                col("FTN_SDO_TRA_AHO_VIV").cast(decimal_pesos),
                col("FTN_SDO_REM_AHO_RET").cast(decimal_pesos),
                col("FTN_SDO_REM_AHO_VIV").cast(decimal_pesos),
                "FTC_LEY_PENSION", "FTC_REGIMEN", "FTC_SEGURO", "FTC_TIPO_PENSION", "FTC_FON_ENTIDAD",
                "FTC_FON_NUMERO_POLIZA", col("FTN_FON_MONTO_TRANSF").cast(decimal_pesos),
                "FTD_FON_FECHA_TRANSF",
                col("FTN_FON_RETENCION_ISR").cast(decimal_pesos), "FTC_AFO_ENTIDAD", "FTC_AFO_MEDIO_PAGO",
                col("FTC_AFO_RECURSOS_ENTREGA").cast(decimal_pesos),
                "FTD_AFO_FECHA_ENTREGA",
                col("FTC_AFO_RETENCION_ISR").cast(decimal_pesos), "FTC_INFNVT_ENTIDAD",
                "FTC_INFNVT_CTA_BANCARIA", col("FTC_INFNVT_RECURSOS_ENTREGA"),
                col("FTC_INFNVT_FECHA_ENTREGA").cast("string"),
                col("FTC_INFNVT_RETENCION_ISR"),
                "FTC_FECHA_EMISION",
                retiros_df.FTC_FECHA_INICIO_PENSION.alias("FTC_FECHA_INICIO_PENSION"),
                col("FTN_PENSION_INSTITUTO_SEG"),
                col("FTN_SALDO_FINAL").cast(decimal_pesos))
        )

        df = df.fillna("").fillna(" ")

        df.dropDuplicates()

        df = (
            df.withColumn("FTC_FECHA_EMISION_1", rpad(col("FTC_FECHA_EMISION_1").cast("string"), 8, " "))
            .withColumn("FTC_NOMBRE", rpad(col("FTC_NOMBRE"), 60, " "))
            .withColumn("FTC_CALLE_NUMERO", rpad(col("FTC_CALLE_NUMERO"), 60, " "))
            .withColumn("FTC_COLONIA", rpad(col("FTC_COLONIA"), 30, " "))
            .withColumn("FTC_MUNICIPIO", rpad(col("FTC_MUNICIPIO"), 60, " "))
            .withColumn("FTC_CP", rpad(col("FTC_CP"), 5, " "))
            .withColumn("FTC_ENTIDAD", rpad(col("FTC_ENTIDAD"), 30, " "))
            .withColumn("FTC_CURP", rpad(col("FTC_CURP"), 18, " "))
            .withColumn("FTC_RFC", rpad(col("FTC_RFC"), 13, " "))
            .withColumn("FTC_NSS", rpad(translate(col("FTC_NSS").cast("string"), ".", ""), 11, "0"))
            .withColumn("FTN_SDO_INI_AHO_RET",
                        lpad(translate(col("FTN_SDO_INI_AHO_RET").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTN_SDO_INI_AHO_VIV",
                        lpad(translate(col("FTN_SDO_INI_AHO_VIV").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTN_SDO_TRA_AHO_RET",
                        lpad(translate(col("FTN_SDO_TRA_AHO_RET").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTN_SDO_TRA_AHO_VIV",
                        lpad(translate(col("FTN_SDO_TRA_AHO_VIV").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTN_SDO_REM_AHO_RET",
                        lpad(translate(col("FTN_SDO_REM_AHO_RET").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTN_SDO_REM_AHO_VIV",
                        lpad(translate(col("FTN_SDO_REM_AHO_VIV").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTC_LEY_PENSION", rpad(col("FTC_LEY_PENSION"), 6, " "))
            .withColumn("FTC_REGIMEN", rpad(col("FTC_REGIMEN"), 2, " "))
            .withColumn("FTC_SEGURO", rpad(col("FTC_SEGURO"), 2, " "))
            .withColumn("FTC_TIPO_PENSION", rpad(col("FTC_TIPO_PENSION"), 2, " "))
            .withColumn("FTC_FON_ENTIDAD", rpad(col("FTC_FON_ENTIDAD"), 16, " "))
            .withColumn("FTC_FON_NUMERO_POLIZA", rpad(col("FTC_FON_NUMERO_POLIZA"), 9, " "))
            .withColumn("FTN_FON_MONTO_TRANSF",
                        lpad(translate(col("FTN_FON_MONTO_TRANSF").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTD_FON_FECHA_TRANSF", rpad(col("FTD_FON_FECHA_TRANSF").cast("string"), 8, " "))
            .withColumn("FTN_FON_RETENCION_ISR",
                        lpad(translate(col("FTN_FON_RETENCION_ISR").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTC_AFO_ENTIDAD", rpad(col("FTC_AFO_ENTIDAD"), 20, " "))
            .withColumn("FTC_AFO_MEDIO_PAGO", rpad(col("FTC_AFO_MEDIO_PAGO"), 15, " "))
            .withColumn("FTC_AFO_RECURSOS_ENTREGA",
                        lpad(translate(col("FTC_AFO_RECURSOS_ENTREGA").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTD_AFO_FECHA_ENTREGA", rpad(col("FTD_AFO_FECHA_ENTREGA"), 8, " "))
            .withColumn("FTC_AFO_RETENCION_ISR",
                        lpad(translate(col("FTC_AFO_RETENCION_ISR").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTC_INFNVT_ENTIDAD", rpad(col("FTC_INFNVT_ENTIDAD"), 14, " "))
            .withColumn("FTC_INFNVT_CTA_BANCARIA",
                        lpad(translate(col("FTC_INFNVT_CTA_BANCARIA").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTC_INFNVT_RECURSOS_ENTREGA",
                        lpad(translate(col("FTC_INFNVT_RECURSOS_ENTREGA").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTC_INFNVT_FECHA_ENTREGA", rpad(col("FTC_INFNVT_FECHA_ENTREGA"), 8, " "))
            .withColumn("FTC_INFNVT_RETENCION_ISR",
                        lpad(translate(col("FTC_INFNVT_RETENCION_ISR").cast("string"), ".", ""),  10, "0"))
            .withColumn("FTC_FECHA_EMISION", rpad(col("FTC_FECHA_EMISION").cast("string"), 8, " "))
            .withColumn("FTC_FECHA_INICIO_PENSION", rpad(col("FTC_FECHA_INICIO_PENSION").cast("string"), 8, " "))
            .withColumn("FTN_PENSION_INSTITUTO_SEG", rpad(col("FTN_PENSION_INSTITUTO_SEG"), 13, " "))
            .withColumn("FTN_SALDO_FINAL",
                        lpad(translate(col("FTN_SALDO_FINAL").cast("string"), ".", ""), 10, "0"))
        )

        df.show(5)

        df = df.dropDuplicates()

        df = df.fillna("").fillna(" ")

        regimenes = ["73", "97"]
        for regimen in regimenes:
            df_regimen = df.filter(col("FTC_ARCHIVO") == regimen)
            retiros_count = df_regimen.count()
            print(retiros_count)
            total = df_regimen.count()
            limit = 500000
            num_parts = math.ceil(total / limit)
            processed_data = df_regimen.rdd.flatMap(lambda row: [([row[i] for i in range(len(row))])]).collect()

            for part in range(1, num_parts + 1):
                if len(processed_data) < limit:
                    processed_data_part = processed_data[:len(processed_data)]
                else:
                    processed_data_part = processed_data[:limit - 1]
                    del processed_data[:limit - 1]

                res = "\n".join("".join(str(item) for item in row) for row in processed_data_part)
                name = f"retiros_mensual_{regimen}_{str(term_id)[:4]}_{str(term_id)[-2:]}_{part}-{num_parts}.txt"
                str_to_gcs(res, name)
                #upload_file_to_smb(smb_remote_file_path, name, res)
                body_message += f"Se generó el archivo de {name} con un total de {retiros_count} registros\n"
                processed_data = [i for i in processed_data if i not in processed_data_part]


        headers = get_headers()  # Get the headers using the get_headers() function
        response = requests.get(url, headers=headers)  # Pass headers with the request
        print(response)

        if response.status_code == 200:
            content = response.content.decode('utf-8')
            data = json.loads(content)
            print('Solicitud fue exitosa')

        else:
            print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")

        #send_email( to_address=correo, subject="Generacion de los archivos de recaudacion",body=body_message)


        notify(
            postgres,
            "Generacion layout correspondencia",
            phase,
            area,
            term=term_id,
            message="Se terminaron de generar las muestras de los estados de cuenta con éxito",
            aprobar=False,
            descarga=False,
        )