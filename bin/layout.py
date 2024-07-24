from profuturo.common import define_extraction, register_time, notify
from profuturo.database import get_postgres_pool,get_postgres_oci_pool, configure_postgres_spark, get_bigquery_pool, configure_postgres_oci_spark
from profuturo.extraction import read_table_insert_temp_view, extract_terms, _get_spark_session, _create_spark_dataframe
from pyspark.sql.functions import concat, col, row_number, lit, lpad, udf,date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import DecimalType
from google.cloud import storage, bigquery
from profuturo.env import load_env
from sqlalchemy import text
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
print(prefix)
url = os.getenv("URL_CORRESPONDENSIA_RECA")
print(url)

correo = os.getenv("CORREO_CORRESPONDENCIA")
print(correo)

decimal_pesos = DecimalType(16, 2)
decimal_udi = DecimalType(16, 6)





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
    blob = bucket.blob(f"correspondenciaReca/{name}")
    blob.upload_from_string(data.encode("iso_8859_1"), content_type="text/plain")
    print(F"SE CREO EL ARCHIVO DE correspondencia/{name}")

def send_email( to_address, subject, body):

    message = "{}\n\n{}".format(subject, body)
    datos = {
        'to': to_address,
        'subject': subject,
        'text': message
    }
    response = requests.post(url, data=datos)

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
    spark = _get_spark_session(
    excuetor_memory = '8g',
    memory_overhead ='1g',
    memory_offhead ='1g',
    driver_memory ='4g',
    intances = 4,
    parallelims = 9000)

    spark.conf.set("spark.sql.shuffle.partitions", 100)
    spark.conf.set("spark.default.parallelism", 100)

    with register_time(postgres_pool, phase, term_id, user, area):
        body_message = ""

        fechas = postgres.execute(text("""
                    SELECT "FTD_FECHA_MOV_INICIO", "FTD_FECHA_MOV_FIN" FROM "ESTADO_CUENTA"."TTMUESTR_GENERAL"
                    limit 1 
                    """)).fetchone()[:]

        (fecha1,fecha2) = fechas

        fecha1 = fecha1.strftime("%d%m%Y")
        fecha2 = fecha2.strftime("%d%m%Y")


        query = f"""
        SELECT
        DISTINCT
        1 AS "NUMERO_SECCION",
        "FCN_NUMERO_CUENTA",
        null as "FCN_FOLIO",
        "FTC_NOMBRE_COMPLETO",
        "FTC_CALLE_NUMERO",
        "FTC_COLONIA",
        "FTC_DELEGACION",
        "FTN_CP",
        "FTC_ENTIDAD_FEDERATIVA",
        "FTC_RFC",
        "FTC_NSS",
        "FTC_CURP",
        to_char("FTD_FECHA_GRAL_INICIO", 'ddmmyyyy') AS "FTD_FECHA_GRAL_INICIO",
        to_char("FTD_FECHA_GRAL_FIN",'ddmmyyyy') AS "FTD_FECHA_GRAL_FIN",
        CASE
        WHEN "FTC_TIPOGENERACION" = 'IMSS - ISSSTE (MIXTO)' THEN 'MIX'
        WHEN "FTC_TIPOGENERACION" = 'GENERACIÓN AFORE' THEN 'GAF'
        WHEN "FTC_TIPOGENERACION" = 'GENERACIÓN DE TRANSICIÓN' THEN 'GTR'
        ELSE 'DEC' END "FTC_GENERACION",
        "FTC_DESC_SIEFORE",
        to_char("FTD_FECHA_CORTE", 'ddmmyyyy') AS "FTD_FECHA_CORTE",
        'SIN BLOQUEO' AS "BLOQUEO_IMPRESION",
        "FTN_SALDO_SUBTOTAL",
        "FTN_SALDO_TOTAL",
        "FTN_PENSION_MENSUAL",
        "FTC_TIPO_TRABAJADOR",
        "FTC_FORMATO" --TO_DO CAMBIAR POR PERIODICIDAD DE FOMRATO
        FROM "ESTADO_CUENTA"."TTEDOCTA_GENERAL" G
        LEFT JOIN "ESTADO_CUENTA"."TTEDOCTA_CLIENTE_INDICADOR" I
        ON "FCN_NUMERO_CUENTA" = "FCN_CUENTA"
        --WHERE "FCN_NUMERO_CUENTA" IN {cuentas}
        """
        general_df = _create_spark_dataframe(spark, configure_postgres_oci_spark, query,
        params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        general_df = general_df.withColumn("FCN_NUMERO_CUENTA", lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0"))

        general_df = general_df.repartition("FTC_GENERACION","FTC_ENTIDAD_FEDERATIVA")

        general_df.show()


        query_anverso = f"""
        SELECT 
        *
        FROM (
        SELECT 
        DISTINCT
        CASE
        WHEN "FTC_SECCION" = 'SDO' THEN 4
        WHEN "FTC_SECCION" IN( 'AHO', 'PEN') THEN 2
        WHEN "FTC_SECCION" = 'BON' THEN 3
        END  "NUMERO_SECCION",
        "FTC_SECCION",
        "FTN_ORDEN_SDO",
        "FCN_NUMERO_CUENTA",
        "FTC_CONCEPTO_NEGOCIO",
        "FTN_SALDO_ANTERIOR",
        "FTF_APORTACION",
        "FTN_RETIRO",
        "FTN_RENDIMIENTO",
        "FTN_COMISION",
        "FTN_SALDO_FINAL",
        "FTN_VALOR_ACTUAL_UDI",
        "FTN_VALOR_NOMINAL_UDI",
        "FTN_VALOR_ACTUAL_PESO",
        "FTN_VALOR_NOMINAL_PESO",
        CASE
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%RET%' THEN 'AHORET'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%FOVIS%' THEN 'AHOFOV'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%VIV%' THEN 'AHOVIV'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%VOL%' THEN 'AHOVOL'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%SAR%' THEN 'SAR92'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%VEJEZ%' THEN 'IMSSCE'
        WHEN "FTC_SECCION" = 'SDO' AND "FTC_CONCEPTO_NEGOCIO" LIKE '%ISSSTE 2008%' THEN 'ISSSTE08'
        END "GRUPO_CONCEPTO"
        FROM "ESTADO_CUENTA"."TTEDOCTA_ANVERSO" A
        LEFT JOIN "ESTADO_CUENTA"."TTEDOCTA_CLIENTE_INDICADOR" I
        ON "FCN_NUMERO_CUENTA" = "FCN_CUENTA"
        --WHERE --"FTB_IMPRESION" = TRUE
        --AND "FTB_GENERACION" = TRUE
        --AND "FTB_ENVIO" = FALSE
        --AND I."FCN_ID_PERIODO" = :term
        --AND "FCN_NUMERO_CUENTA" IN {cuentas}
        ) X
        ORDER BY "FCN_NUMERO_CUENTA","NUMERO_SECCION","FTC_SECCION", "FTN_ORDEN_SDO"
        """
        anverso_df =_create_spark_dataframe(spark, configure_postgres_oci_spark, query_anverso,params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        anverso_seccion_ahor = anverso_df.select(col("NUMERO_SECCION").alias("NUMERO_SECCION_AHO"),lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0").alias("FCN_NUMERO_CUENTA_AHO"),
                                                 col("FTC_CONCEPTO_NEGOCIO").alias("FTC_CONCEPTO_NEGOCIO_AHO"),
                                                 col("FTN_SALDO_ANTERIOR"),col("FTF_APORTACION"),col("FTN_RETIRO"),
                                                 col("FTN_RENDIMIENTO"),
                                                 col("FTN_COMISION"),
                                                 col("FTN_SALDO_FINAL")
                                                 ).filter(col("NUMERO_SECCION") == 2)

        anverso_seccion_ahor = anverso_seccion_ahor.repartition("FTC_CONCEPTO_NEGOCIO")

        anverso_seccion_ahor.show(50)

        anverso_seccion_bono = anverso_df.select(col("NUMERO_SECCION").alias("NUMERO_SECCION_BON"),lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0").alias("FCN_NUMERO_CUENTA_BON"),
                                                 col("FTC_CONCEPTO_NEGOCIO").alias("FTC_CONCEPTO_NEGOCIO_BON"), col("FTN_VALOR_ACTUAL_UDI"),
                                                col("FTN_VALOR_NOMINAL_UDI"), col("FTN_VALOR_ACTUAL_PESO"),
                                                col("FTN_VALOR_NOMINAL_PESO")).filter(col("NUMERO_SECCION") == 3)

        anverso_seccion_bono.show(50)

        anverso_seccion_sdo = anverso_df.select(col("NUMERO_SECCION").alias("NUMERO_SECCION_SDO"),lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0").alias("FCN_NUMERO_CUENTA_SDO"),
                                                col("FTC_CONCEPTO_NEGOCIO").alias("FTC_CONCEPTO_NEGOCIO_SDO"),col("GRUPO_CONCEPTO"),
                                                col("FTN_SALDO_FINAL").alias("FTN_SALDO_FINAL_SDO")
                                                ).filter(col("NUMERO_SECCION") == 4)

        anverso_seccion_sdo = anverso_seccion_sdo.repartition("FTC_CONCEPTO_NEGOCIO","GRUPO_CONCEPTO")

        anverso_seccion_sdo.show(50)

        joined_anverso = (
            general_df
            .join(anverso_seccion_ahor,general_df.FCN_NUMERO_CUENTA == anverso_seccion_ahor.FCN_NUMERO_CUENTA_AHO, 'LEFT')
            .join(anverso_seccion_bono, general_df.FCN_NUMERO_CUENTA == anverso_seccion_bono.FCN_NUMERO_CUENTA_BON,
                  'LEFT')
            .join(anverso_seccion_sdo, general_df.FCN_NUMERO_CUENTA == anverso_seccion_sdo.FCN_NUMERO_CUENTA_SDO,
                  'LEFT')
        )

        joined_anverso = joined_anverso.withColumn("id", monotonically_increasing_id())

        # Obtener el número total de registros
        total_records = joined_anverso.count()

        # Definir el tamaño de la partición
        partition_size = 500_000

        # Inicializar la variable de índice de inicio
        start_index = 0
        part = 1

        while start_index < total_records:
            # Seleccionar los registros de la partición actual
            partition = joined_anverso.limit(partition_size).filter(joined_anverso.id >= start_index)

            general_df = joined_anverso.select(
                col("FCN_NUMERO_CUENTA").alias("FCN_NUMERO_CUENTA"),
                col("FCN_FOLIO").alias("FCN_FOLIO"),
                col("FTC_NOMBRE_COMPLETO").alias("FTC_NOMBRE_COMPLETO"),
                col("FTC_CALLE_NUMERO").alias("FTC_CALLE_NUMERO"),
                col("FTC_COLONIA").alias("FTC_COLONIA"),
                col("FTC_DELEGACION").alias("FTC_DELEGACION"),
                col("FTN_CP").alias("FTN_CP"),
                col("FTC_ENTIDAD_FEDERATIVA").alias("FTC_ENTIDAD_FEDERATIVA"),
                col("FTC_RFC").alias("FTC_RFC"),
                col("FTC_NSS").alias("FTC_NSS"),
                col("FTC_CURP").alias("FTC_CURP"),
                col("FTD_FECHA_GRAL_INICIO").alias("FTD_FECHA_GRAL_INICIO"),
                col("FTD_FECHA_GRAL_FIN").alias("FTD_FECHA_GRAL_FIN"),
                col("FTC_GENERACION").alias("FTC_GENERACION"),
                col("FTC_DESC_SIEFORE").alias("FTC_DESC_SIEFORE"),
                col("FTD_FECHA_CORTE").alias("FTD_FECHA_CORTE"),
                col("BLOQUEO_IMPRESION").alias("BLOQUEO_IMPRESION"),
                col("FTN_SALDO_SUBTOTAL").cast(decimal_pesos).alias("FTN_SALDO_SUBTOTAL"),
                col("FTN_SALDO_TOTAL").cast(decimal_pesos).alias("FTN_SALDO_TOTAL"),
                col("FTN_PENSION_MENSUAL").cast(decimal_pesos).alias("FTN_PENSION_MENSUAL"),
                col("FTC_TIPO_TRABAJADOR").alias("FTC_TIPO_TRABAJADOR"),
                col("FTC_FORMATO").alias("FTC_FORMATO")
            )

            general_df = general_df.dropDuplicates()

            general_df.show(50)

            anverso_seccion_ahor = joined_anverso.select(
                                                     col("FCN_NUMERO_CUENTA_AHO"),
                                                     col("FTC_CONCEPTO_NEGOCIO_AHO"),
                                                     col("FTN_SALDO_ANTERIOR").cast(decimal_pesos),
                                                     col("FTF_APORTACION").cast(decimal_pesos),
                                                     col("FTN_RETIRO").cast(decimal_pesos),
                                                     col("FTN_RENDIMIENTO").cast(decimal_pesos),
                                                     col("FTN_COMISION").cast(decimal_pesos),
                                                     col("FTN_SALDO_FINAL").cast(decimal_pesos)
            )

            anverso_seccion_ahor = anverso_seccion_ahor.dropDuplicates()
            anverso_seccion_ahor = anverso_seccion_ahor.filter(col("FCN_NUMERO_CUENTA_AHO").isNotNull())

            anverso_seccion_ahor.show(50)

            anverso_seccion_bono = joined_anverso.select(
                                                     col("FCN_NUMERO_CUENTA_BON"),
                                                     col("FTC_CONCEPTO_NEGOCIO_BON"), col("FTN_VALOR_ACTUAL_UDI").cast(decimal_udi),
                                                     col("FTN_VALOR_NOMINAL_UDI").cast(decimal_udi), col("FTN_VALOR_ACTUAL_PESO").cast(decimal_pesos),
                                                     col("FTN_VALOR_NOMINAL_PESO").cast(decimal_pesos))

            anverso_seccion_bono = anverso_seccion_bono.dropDuplicates()
            anverso_seccion_bono = anverso_seccion_bono.filter(col("FCN_NUMERO_CUENTA_BON").isNotNull())

            anverso_seccion_bono.show()

            anverso_seccion_sdo = joined_anverso.select(
                                                    col("FCN_NUMERO_CUENTA_SDO"),
                                                    col("FTC_CONCEPTO_NEGOCIO_SDO"), col("GRUPO_CONCEPTO"),
                                                    col("FTN_SALDO_FINAL_SDO").cast(decimal_pesos)
                                                    )

            anverso_seccion_sdo = anverso_seccion_sdo.dropDuplicates()
            anverso_seccion_sdo = anverso_seccion_sdo.filter(col("FCN_NUMERO_CUENTA_SDO").isNotNull())

            anverso_seccion_sdo.show()

            general_count = general_df.count()
            ahorro_count = anverso_seccion_ahor.count()
            bono_count = anverso_seccion_bono.count()
            saldo_count = anverso_seccion_sdo.count()
            total_count = sum([general_count, ahorro_count, bono_count, saldo_count])
            print("COUNTERS: ", general_count, ahorro_count, bono_count, saldo_count, total_count)

            processed_data = []
            processed_data += process_dataframe(general_df.fillna("").fillna(0), 1)
            processed_data += process_dataframe(anverso_seccion_ahor.fillna("").fillna(0), 2)
            processed_data += process_dataframe(anverso_seccion_bono.fillna("").fillna(0), 3)
            processed_data += process_dataframe(anverso_seccion_sdo.fillna("").fillna(0), 4)
            print("DATOS PROCESADOS ANVERSO")

            processed_data_part = processed_data
            res = "\n".join("|".join(str(item) for item in row) for row in processed_data_part)
            final_row = f"\n5|{general_count}|{ahorro_count}|{bono_count}|{saldo_count}|{total_count}"
            res = res + final_row

            name_anverso = f"recaudacion_anverso_cuatrimestral_{term_id}_{part}.txt"

            str_to_gcs(res, name_anverso)
            # upload_file_to_sftp(sftp_host, sftp_user, sftp_pass, name_anverso, sftp_remote_file_path, res)
            body_message += f"Se generó el archivo de {name_anverso} con un total de {general_count} registros\n"

            # Actualizar el índice de inicio para la próxima partición
            start_index += partition_size

            part += 1

        query_reverso = f"""
                        SELECT * FROM (SELECT DISTINCT * FROM "ESTADO_CUENTA"."TTEDOCTA_REVERSO"  --WHERE "FCN_NUMERO_CUENTA" IN {cuentas}
                        ) X
                        ORDER BY  "FCN_NUMERO_CUENTA", "FTC_SECCION", "FTN_ORDEN"
                                        """
        read_table_insert_temp_view(
            configure_postgres_oci_spark,
            query_reverso,
            "reverso"
        )

        df_edo_reverso = spark.sql("SELECT * FROM reverso")
        df_edo_reverso = df_edo_reverso.withColumn("FCN_NUMERO_CUENTA", lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0"))

        conteo_reverso = df_edo_reverso.count()
        print("SE EXTRAJO EXITOSAMENTE REVERSO", conteo_reverso)

        df_reverso_general = df_edo_reverso.join(general_df, "FCN_NUMERO_CUENTA")
        print("SE REALIZO EL JOIN DE REVERSO", df_reverso_general.count())
        df_reverso_general = (
            df_reverso_general.select("FCN_NUMERO_CUENTA", "FTN_ID_CONCEPTO", "FTC_SECCION",
                                      date_format("FTD_FECHA_MOVIMIENTO", "ddMMyyyy").alias(
                                          "FTD_FECHA_MOVIMIENTO"),
                                      "FTC_DESC_CONCEPTO", "FTC_PERIODO_REFERENCIA", "FTN_DIA_COTIZADO",
                                      col("FTN_SALARIO_BASE").cast("decimal(16, 2)"),
                                      col("FTN_MONTO").cast("decimal(16, 2)"))
        )
        print("DF_REVERSO_GENERAL", df_reverso_general.count())
        ret = df_reverso_general.filter(col("FTC_SECCION") == "RET").count()
        print("RET", ret)
        vol = df_reverso_general.filter(col("FTC_SECCION") == "VOL").count()
        print("VOL", vol)
        viv = df_reverso_general.filter(col("FTC_SECCION") == "VIV").count()
        print("VIV", viv)
        total = df_reverso_general.count()

        num_parts_reverso = math.ceil(total / partition_size)

        reverso_data = process_dataframe(df_reverso_general.fillna("").fillna(""), 1)
        print("DATOS PROCESADOS REVERSO")

        for part in range(1, num_parts_reverso + 1):
            if len(reverso_data) < partition_size:
                reverso_data_part = reverso_data
                res = "\n".join("|".join(str(item) if item is not None else '' for item in row) for row in reverso_data_part)
                reverso_final_row = f"\n2|{ret}|{vol}|{viv}|{total}|{fecha1}|{fecha2}"
                res = res + reverso_final_row
            else:
                reverso_data_part = reverso_data[:partition_size]
                res = "\n".join("|".join(str(item) for item in row) for row in reverso_data_part)
                del reverso_data[:partition_size - 1]

            name_reverso = f"recaudacion_reverso_cuatrimestral_{term_id}_{part}-{num_parts_reverso}.txt"
            str_to_gcs(res, name_reverso)
            # upload_file_to_sftp(sftp_host, sftp_user, sftp_pass, name_reverso, "", res)
            body_message += f"Se generó el archivo de {name_reverso} con un total de {conteo_reverso} registros\n"

        headers = get_headers()  # Get the headers using the get_headers() function
        response = requests.get(url, headers=headers)  # Pass headers with the request
        print(response)

        if response.status_code == 200:
            content = response.content.decode('utf-8')
            data = json.loads(content)
            print('Solicitud fue exitosa')

        else:
            print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")

        send_email(
            to_address=correo,
            subject="Generacion de los archivos de recaudacion",
            body=body_message
        )


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





