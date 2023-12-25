from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool
from profuturo.extraction import extract_terms, _get_spark_session
from profuturo.reporters import HtmlReporter
from google.cloud import storage, bigquery
import pyspark.sql.functions as f
import sys

spark = _get_spark_session()

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

storage_client = storage.Client()
bigquery_client = bigquery.Client()
bigquery_project = bigquery_client.project


def get_buckets():
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        if bucket.name.startswith("edo_cuenta_profuturo"):
            return bucket.name


bucket = storage_client.get_bucket(get_buckets())


def extract_bigquery(table):
    df = spark.read.format('bigquery') \
        .option('table', f'{bigquery_project}:{table}') \
        .load()
    print(f"SE EXTRAIDO EXITOSAMENTE {table}")
    return df


def process_dataframe(df, identifier):
    return df.rdd.flatMap(lambda row: [([identifier] + [row[i] for i in range(len(row))])]).collect()


def str_to_gcs(data, name, term_id):
    blob = bucket.blob(f"correspondencia/{name}_{term_id}.txt")
    blob.upload_from_string(data)


with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):
        df_indicador = (
            extract_bigquery('ESTADO_CUENTA.TTEDOCTA_CLIENTE_INDICADOR')
            .filter((f.col("FTB_IMPRESION") == True) & (f.col("FTB_ENVIO") == False))
        )
        df_general = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_GENERAL').filter(f"FCN_ID_PERIODO == {term_id}")
        df_general = df_general.join(df_indicador, df_general.FCN_ID_EDOCTA == df_indicador.FCN_CUENTA,
                                     'left')

        df_anverso = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_ANVERSO').withColumnRenamed("FCN_NUMERO_CUENTA",
                                                                                          "FCN_NUMERO_CUENTA_ANVERSO")

        df_anverso_general = df_anverso.join(df_general, "FCN_ID_EDOCTA").fillna(value="")

        df_general = (
            df_general.withColumn("FTD_FECHA_GRAL_INICIO", f.to_date(f.col("FTD_FECHA_GRAL_INICIO"), "ddMMyyyy"))
            .withColumn("FTD_FECHA_GRAL_FIN", f.to_date(f.col("FTD_FECHA_GRAL_FIN"), "ddMMyyyy"))
            .withColumn("FTD_FECHA_CORTE", f.to_date(f.col("FTD_FECHA_CORTE"), "ddMMyyyy"))
        )

        unique_clients = df_anverso_general.select("FCN_ID_EDOCTA").distinct().rdd.collect()
        clients = [row.FCN_ID_EDOCTA for row in unique_clients]
        df_general = df_general.withColumn("vacio", f.lit(None))

        df_general = (
            df_general.filter(f.col("FCN_ID_EDOCTA").isin(clients))
            .select("FCN_NUMERO_CUENTA", "FCN_FOLIO", "FTC_NOMBRE_COMPLETO", "FTC_CALLE_NUMERO",
                    "FTC_COLONIA", "FTC_DELEGACION", "FTN_CP", "FTC_ENTIDAD_FEDERATIVA", "FTC_RFC",
                    "FTC_NSS", "FTC_CURP",
                    f.date_format("FTD_FECHA_GRAL_INICIO", "ddMMyyyy").alias("FTD_FECHA_GRAL_INICIO"),
                    f.date_format("FTD_FECHA_GRAL_FIN", "ddMMyyyy").alias("FTD_FECHA_GRAL_FIN"),
                    "FTN_ID_FORMATO",
                    "FTN_ID_SIEFORE", f.date_format("FTD_FECHA_CORTE", "ddMMyyyy").alias("FTD_FECHA_CORTE"), "vacio",
                    f.col("FTN_SALDO_SUBTOTAL").cast("decimal(16, 2)"),
                    f.col("FTN_SALDO_TOTAL").cast("decimal(16, 2)"),
                    f.col("FTN_PENSION_MENSUAL").cast("decimal(16, 2)"), "FTC_TIPO_TRABAJADOR",
                    "FTC_FORMATO"
                    )
        )

        df_anverso_aho = (
            df_anverso_general.filter(
                (f.col("FTC_SECCION").isin(["AHO", "PEN"])) & (f.col("FCN_ID_EDOCTA").isin(clients)))
            .select("FCN_NUMERO_CUENTA_ANVERSO", "FTC_CONCEPTO_NEGOCIO",
                    f.col("FTN_SALDO_ANTERIOR").cast("decimal(16, 2)"),
                    f.col("FTF_APORTACION").cast("decimal(16, 2)"),
                    f.col("FTN_RETIRO").cast("decimal(16, 2)"),
                    f.col("FTN_RENDIMIENTO").cast("decimal(16, 2)"),
                    f.col("FTN_COMISION").cast("decimal(16, 2)"),
                    f.col("FTN_SALDO_FINAL").cast("decimal(16, 2)"))

        )

        df_anverso_bon = (
            df_anverso_general.filter((f.col("FTC_SECCION") == "BON") & (f.col("FCN_ID_EDOCTA").isin(clients)))
            .select("FCN_NUMERO_CUENTA_ANVERSO", "FTC_CONCEPTO_NEGOCIO",
                    f.col("FTN_VALOR_ACTUAL_UDI").cast("decimal(16, 2)"),
                    f.col("FTN_VALOR_NOMINAL_UDI").cast("decimal(16, 2)"),
                    f.col("FTN_VALOR_ACTUAL_PESO").cast("decimal(16, 2)"),
                    f.col("FTN_VALOR_NOMINAL_PESO").cast("decimal(16, 2)"))

        )

        df_anverso_sdo = (
            df_anverso_general.filter((f.col("FTC_SECCION") == "SDO") & (f.col("FCN_ID_EDOCTA").isin(clients)))
            .select("FCN_NUMERO_CUENTA_ANVERSO", "FTC_GRUPO_CONCEPTO", "FTC_CONCEPTO_NEGOCIO",
                    f.col("FTN_SALDO_FINAL").cast("decimal(16, 2)"))

        )

        general_count = df_general.count()
        ahorro_count = df_anverso_aho.count()
        bono_count = df_anverso_bon.count()
        saldo_count = df_anverso_sdo.count()

        processed_data = []
        processed_data += process_dataframe(df_general, 1)
        processed_data += process_dataframe(df_anverso_aho, 2)
        processed_data += process_dataframe(df_anverso_bon, 3)
        processed_data += process_dataframe(df_anverso_sdo, 4)

        res = "\n".join("|".join(str(item) for item in row) for row in processed_data)

        total_count = sum([general_count, ahorro_count, bono_count, saldo_count])

        final_row = f"\n5|{general_count}|{ahorro_count}|{bono_count}|{saldo_count}|{total_count}"
        data_strings = res + final_row

        str_to_gcs(data_strings, "recaudacion_anverso", term_id)

        df_edo_reverso = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_REVERSO')

        df_reverso_general = df_edo_reverso.join(df_general, "FCN_NUMERO_CUENTA")
        df_reverso_general = (
            df_reverso_general.select("FCN_NUMERO_CUENTA", "FTN_ID_CONCEPTO", "FTC_SECCION",
                                      f.date_format("FTD_FECHA_MOVIMIENTO", "ddMMyyyy").alias("FTD_FECHA_MOVIMIENTO"),
                                      "FTC_DESC_CONCEPTO", "FTC_PERIODO_REFERENCIA", "FTN_DIA_COTIZADO",
                                      f.col("FTN_SALARIO_BASE").cast("decimal(16, 2)"),
                                      f.col("FTN_MONTO").cast("decimal(16, 2)"))
        )

        reverso_data = process_dataframe(df_reverso_general, 1)

        res = "\n".join("|".join(str(item) for item in row) for row in reverso_data)

        ret = df_reverso_general.filter(f.col("FTC_SECCION") == "RET").count()
        vol = df_reverso_general.filter(f.col("FTC_SECCION") == "VOL").count()
        viv = df_reverso_general.filter(f.col("FTC_SECCION") == "VIV").count()
        total = df_reverso_general.count()
        reverso_final_row = f"\n2|{ret}|{vol}|{viv}|{total}"
        final_reverso = res + reverso_final_row
        str_to_gcs(final_reverso, "recaudacion_reverso", term_id)

        notify(
            postgres,
            f"Generacion de archivos Recaudaciones",
            phase,
            area,
            term=term_id,
            message=f"Se han exportado recaudaciones para el periodo {time_period}",
            aprobar=False,
            descarga=False
        )
