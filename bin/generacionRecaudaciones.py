from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool
from profuturo.extraction import extract_terms, _get_spark_session
from profuturo.reporters import HtmlReporter
from pyspark.sql import SparkSession
from google.cloud import storage
import pyspark.sql.functions as f
import sys

spark = _get_spark_session()

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

storage_client = storage.Client()
bucket = storage_client.get_bucket("edo_cuenta_profuturo_dev_b")


def extract_bigquery(table):
    df = spark.read.format('bigquery') \
        .option('table', f'estado-de-cuenta-service-dev-b:{table}') \
        .load()
    return df


def get_data(index, columns, df):
    df = df.select(*columns)
    data = df.rdd.flatMap(lambda row: [f"{row[column]}" for column in reverso_columns]).collect()
    data = [list(data[i:i + len(columns)]) for i in range(0, len(data), len(columns))]
    res = f"{index}\n"
    for row in data:
        data_str = "|".join(row[i] for i in range(len(reverso_columns)))
        res += data_str + "\n"

    return res, len(data)


def str_to_gcs(data, name, term_id):
    blob = bucket.blob(f"test_retiros/{name}_{term_id}.txt")
    blob.upload_from_string(data)


with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):

        general_columns = ["FCN_FOLIO", "FTC_NOMBRE_COMPLETO", "FTC_CALLE_NUMERO", "FTC_COLONIA", "FTC_DELEGACION",
                           "FTN_CP", "FTC_ENTIDAD_FEDERATIVA", "FTC_RFC", "FTC_NSS", "FTC_CURP",
                           "FTD_FECHA_GRAL_INICIO", "FTD_FECHA_GRAL_FIN", "FTN_ID_FORMATO", "FTN_ID_SIEFORE",
                           "FTD_FECHA_CORTE", "FTB_PDF_IMPRESO", "FTF_SALDO_SUBTOTAL", "FTF_SALDO_TOTAL", "FTN_PENSION_MENSUAL"]
        ahorro_columns = ["FTC_CONCEPTO_NEGOCIO", "FTF_SALDO_ANTERIOR", "FTF_APORTACION", "FTF_RETIRO", "FTF_RENDIMIENTO",
                          "FTF_COMISION", "FTF_SALDO_FINAL"]
        bono_columns = ["FTC_CONCEPTO_NEGOCIO", "FTF_VALOR_ACTUAL_UDI", "FTF_VALOR_NOMINAL_UDI", "FTF_VALOR_ACTUAL_PESO",
                        "FTF_VALOR_NOMINAL_PESO"]
        saldo_columns = ["FTN_ID_CONCEPTO", "FTC_CONCEPTO_NEGOCIO", "FTF_SALDO_TOTAL"]

        df_edo_general = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_GENERAL').filter(f"FCN_ID_PERIODO == {term_id}")
        df_edo_anverso = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_ANVERSO')

        df_edo_reverso = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_REVERSO')

        df_general_anverso = df_edo_general.join(df_edo_anverso, 'FCN_ID_EDOCTA')
        data_general, general_count = get_data(1, general_columns, df_general_anverso)
        data_ahorro, ahorro_count = get_data(2, ahorro_columns, df_general_anverso)
        data_bono, bono_count= get_data(3, bono_columns, df_general_anverso)
        data_saldo, saldo_count = get_data(4, saldo_columns, df_general_anverso)

        total_count = sum([general_count, ahorro_count, bono_count, saldo_count])
        final_row = f"5\n{general_count}|{ahorro_count}|{bono_count}|{saldo_count}|{total_count}|"
        data_strings = data_general + data_ahorro + data_bono + data_saldo + final_row

        str_to_gcs(data_strings, "recaudacion_anverso", term_id)

        reverso_columns = ["FCN_NUMERO_CUENTA", "FTN_ID_CONCEPTO", "FTC_SECCION", "FTD_FECHA_MOVIMIENTO",
                           "FTC_DESC_CONCEPTO", "FTC_PERIODO_REFERENCIA", "FTN_DIA_COTIZADO", "FTN_SALARIO_BASE",
                           "FTN_MONTO"]
        reverso_data, reverso_count = get_data(1, reverso_columns, df_edo_reverso)
        ret = df_edo_reverso.filter(f.col("FTC_SECCION") == "RET").count()
        vol = df_edo_reverso.filter(f.col("FTC_SECCION") == "VOL").count()
        viv = df_edo_reverso.filter(f.col("FTC_SECCION") == "VIV").count()
        total = df_edo_reverso.count()
        reverso_final_row = f"2\n{ret}|{vol}|{viv}|{total}|"
        final_reverso = reverso_data + reverso_final_row
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