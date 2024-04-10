from profuturo.extraction import read_table_insert_temp_view, extract_terms, _get_spark_session, _create_spark_dataframe
from profuturo.database import get_postgres_pool,get_postgres_oci_pool, configure_postgres_oci_spark, get_bigquery_pool
from pyspark.sql.functions import concat, col, row_number, lit, lpad, udf,date_format, rpad, translate
from pyspark.sql.functions import monotonically_increasing_id
from profuturo.common import define_extraction, register_time
from google.cloud import storage, bigquery
from pyspark.sql.types import DecimalType
from profuturo.env import load_env
import requests
import sys
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
url = os.getenv("URL_MUESTRAS_RECA")
print(url)

decimal_pesos = DecimalType(16, 2)


cuentas = (10000851,10000861,10000868,10000872,1330029515,1350011161,1530002222,1700004823,3070006370,3200089837,
           3200231348,3200534369,3201895226,3201900769,3202077144,3202135111,3300118473,3300576485,3300797221,3300809724,
           3400764001,3500053269,3500058618,6120000991,6442107959,6442128265,6449009395,6449015130,10000884,10000885,
           10000887,10000888,10000889,10000890,10000891,10000892,10000893,10000894,10000895,10000896,10001041,10001042,
           10000898,10000899,10000900,10000901,10000902,10000903,10000904,10000905,10000906,10000907,10000908,10000909,
           10000910,10000911,10000912,10000913,10000914,10000915,10000916,10000917,10000918,10000919,10000920,10000921,
           10000922,10000923,10000924,10000925,10000927,10000928,10000929,10000930,10000931,10000932,10000933,10000934,
           10000935,10000936,10001263,10001264,10001265,10001266,10001267,10001268,10001269,10001270,10001271,10001272,
           10001274,10001275,10001277,10001278,10001279,10001280,10001281,10001282,10001283,10001284,10001285,10001286,
           10001288,10001289,10001290,10001292,10001293,10001294,10001296,10001297,10001298,10001299,10001300,10001301,
           10001305,10001306,10001307,10001308,10001309,10001311,10001312,10001314,10001315,10001316,10001317,10001318,
           10001319,10001320,10001321,10001322,10000896,10000898,10000790,10000791,10000792,10000793,10000794,10000795,
           10000797,10000798,10000799,10000800,10000801,10000802,10000803,10000804,10000805,10000806,10000807,10000808,
           10000809,10000810,10000811,10000812,10000813,10000814,10000815,10000816,10000817,10000818,10000819,10000820,
           10000821,10000822,10000823,10000824,10000825,10000826,10000827,10000828,10000830,10000832,10000833,10000834,
           10000835,10000836,10000837,10000838,10000839,10000840,10001098,10001099,10001100,10001101,10001102,10001103,
           10001104,10001105,10001106,10001107,10001108,10001109,10001110,10001111,10001112,10001113,10001114,10001115,
           10001116,10001117,10001118,10001119,10001120,10001121,10001122,10001123,10001124,10001125,10001126,10001127,
           10001128,10001129,10001130,10001131,10001132,10001133,10001134,10001135,10001136,10001137,10001138,10001139,
           10001140,10001141,10001142,10001143,10001145,10001146,10001147,10001148,10000991,10000992,10000993,10000994,
           10000995,10000996,10000997,10000998,10000999,10001000,10001001,10001002,10001003,10001004,10001005,10001006,
           10001007,10001008,10001009,10001010,10001011,10001012,10001013,10001014,10001015,10001016,10001017,10001018,
           10001019,10001020,10001021,10001023,10001024,10001025,10001026,10001027,10001029,10001030,10001031,10001032,
           10001033,10001034,10001035,10001036,10001037,10001038,10001039,10001040
           )
cuentas = (1330029515,1350011161,1530002222,3070006370,
           3200089837,3200474366,3200534369,3200767640,
           3200840759,3201096947,3201292580,3201900769)
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
        "TFN_FON_RETENCION_ISR", "FTC_AFO_ENTIDAD",
        "FTC_AFO_MEDIO_PAGO", "FTC_AFO_RECURSOS_ENTREGA",
        to_char("FTD_AFO_FECHA_ENTREGA"::timestamp, 'yyyymmdd') as  "FTD_AFO_FECHA_ENTREGA",
        "FTC_AFO_RETENCION_ISR", "FTC_INFNVT_ENTIDAD","FTC_INFNVT_CTA_BANCARIA",
        "FTC_INFNVT_RECURSOS_ENTREGA", "FTC_INFNVT_FECHA_ENTREGA","FTC_INFNVT_RETENCION_ISR",
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
        """

        query_general_retiro = """
        SELECT 
        "FCN_NUMERO_CUENTA", "FCN_ID_PERIODO", "FTC_NOMBRE",
        "FTC_CALLE_NUMERO", "FTC_COLONIA","FTC_MUNICIPIO", 
        "FTC_CP", "FTC_ENTIDAD", "FTC_CURP", "FTC_RFC", 
        "FTC_NSS", "FCN_ID_EDOCTA"
        FROM "ESTADO_CUENTA"."TTEDOCTA_RETIRO_GENERAL"
        """

        retiros_general_df = _create_spark_dataframe(spark, configure_postgres_oci_spark, query_general_retiro,
                                             params={"term": term_id, "start": start_month, "end": end_month,
                                                     "user": str(user)})

        retiros_general_df = retiros_general_df.withColumn("FCN_NUMERO_CUENTA", lpad(col("FCN_NUMERO_CUENTA").cast("string"), 10, "0"))


        retiros_df = _create_spark_dataframe(spark, configure_postgres_oci_spark, query_retiro,
                                                     params={"term": term_id, "start": start_month, "end": end_month,
                                                             "user": str(user)})

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
                col("TFN_FON_RETENCION_ISR").cast(decimal_pesos), "FTC_AFO_ENTIDAD", "FTC_AFO_MEDIO_PAGO",
                col("FTC_AFO_RECURSOS_ENTREGA").cast(decimal_pesos),
                "FTD_AFO_FECHA_ENTREGA",
                col("FTC_AFO_RETENCION_ISR").cast(decimal_pesos), "FTC_INFNVT_ENTIDAD",
                "FTC_INFNVT_CTA_BANCARIA", col("FTC_INFNVT_RECURSOS_ENTREGA").cast(decimal_pesos),
                col("FTC_INFNVT_FECHA_ENTREGA").cast("string"),
                col("FTC_INFNVT_RETENCION_ISR").cast(decimal_pesos),
                "FTC_FECHA_EMISION",
                retiros_df.FTC_FECHA_INICIO_PENSION.alias("FTC_FECHA_INICIO_PENSION"),
                col("FTN_PENSION_INSTITUTO_SEG"),
                col("FTN_SALDO_FINAL").cast(decimal_pesos))
        )

        df = df.fillna("").fillna(0)

        df = (
            df.withColumn("FTC_FECHA_EMISION_1", rpad(col("FTC_FECHA_EMISION_1").cast("string"), 8, " "))
            .withColumn("FTC_NOMBRE", rpad(col("FTC_NOMBRE"), 60, " "))
            .withColumn("FTC_CALLE_NUMERO", rpad(col("FTC_CALLE_NUMERO"), 60, " "))
            .withColumn("FTC_COLONIA", rpad(col("FTC_COLONIA"), 30, " "))
            .withColumn("FTC_MUNICIPIO", rpad(col("FTC_MUNICIPIO"), 60, " "))
            .withColumn("FTC_CP", lpad(translate(col("FTC_CP").cast("string"), ".", ""), 5, "0"))
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
            .withColumn("TFN_FON_RETENCION_ISR",
                        lpad(translate(col("TFN_FON_RETENCION_ISR").cast("string"), ".", ""), 10, "0"))
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
                        lpad(translate(col("FTC_INFNVT_RETENCION_ISR").cast("string"), ".", ""), 10, "0"))
            .withColumn("FTC_FECHA_EMISION", rpad(col("FTC_FECHA_EMISION").cast("string"), 8, " "))
            .withColumn("FTC_FECHA_INICIO_PENSION", rpad(col("FTC_FECHA_INICIO_PENSION").cast("string"), 8, " "))
            .withColumn("FTN_PENSION_INSTITUTO_SEG", rpad(col("FTN_PENSION_INSTITUTO_SEG"), 13, " "))
            .withColumn("FTN_SALDO_FINAL",
                        lpad(translate(col("FTN_SALDO_FINAL").cast("string"), ".", ""), 10, "0"))
        )

        regimenes = ["73", "97"]
        for regimen in regimenes:
            df_regimen = df.filter(col("FTC_REGIMEN") == regimen)
            retiros_count = df_regimen.count()
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

        send_email( to_address="alfredo.guerra@profuturo.com.mx",
        subject="Generacion de los archivos de retiros",
        body=body_message)

        """notify(
            postgres,
            f"Generacion de archivos Retiros",
            phase,
            area,
            term=term_id,
            message=f"Se han exportado retiros para el periodo {term_id}",
            aprobar=False,
            descarga=False
        )"""