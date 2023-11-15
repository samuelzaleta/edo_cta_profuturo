from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool
from profuturo.extraction import extract_terms
from profuturo.reporters import HtmlReporter
from pyspark.sql import SparkSession
import sys

spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('spark-bigquery-recaudaciones') \
    .getOrCreate()

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])


def extract_bigquery(table, period):
    df = spark.read.format('bigquery') \
        .option('table', f'poc-profuturo-estado-de-cuenta:{table}') \
        .load()
    df.filter(df.FCN_ID_PERIODO == period)
    return df


def format_row(row):
    formatted_data = []

    for column in general_columns:
        if row[column] is not None and row[column] != '':
            general_data = [f"{row[column]}" for column in general_columns if row[column] is not None]
            formatted_data.append("1\n" + "|".join(general_data))
            break

    for column in ahorro_columns:
        if row[column] is not None and row[column] != '':
            ahorro_data = [f"{row[column]}" for column in ahorro_columns if row[column] is not None]
            formatted_data.append("2\n" + "|".join(ahorro_data))
            break

    for column in bono_columns:
        if row[column] is not None and row[column] != '':
            bono_data = [f"{row[column]}" for column in bono_columns if row[column] is not None]
            formatted_data.append("3\n" + "|".join(bono_data))
            break

    for column in saldo_columns:
        if row[column] is not None and row[column] != '':
            saldo_data = [f"{row[column]}" for column in saldo_columns if row[column] is not None]
            formatted_data.append("4\n" + "|".join(saldo_data))
            break

    if formatted_data:
        return "\n".join(formatted_data)
    else:
        return None

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
                           "FTD_FECHA_CORTE", "FTF_SALDO_SUBTOTAL", "FTF_SALDO_TOTAL", "FTN_PENSION_MENSUAL"]
        ahorro_columns = ["FTC_DESC_CONCEPTO", "FTF_SALDO_ANTERIOR", "FTF_APORTACION", "FTF_RETIRO", "FTF_RENDIMIENTO",
                          "FTF_COMISION", "FTF_SALDO_FINAL"]
        bono_columns = ["FTC_DESC_CONCEPTO", "FTF_VALOR_ACTUAL_UDI", "FTF_VALOR_NOMINAL_UDI", "FTF_VALOR_ACTUAL_PESO",
                        "FTF_VALOR_NOMINAL_PESO"]
        saldo_columns = ["FTN_ID_CONCEPTO", "FTC_DESC_CONCEPTO", "FTF_SALDO_TOTAL"]

        df_edo_general = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_GENERAL', term_id)
        df_edo_anverso = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_ANVERSO', term_id)

        df = df_edo_general.join(df_edo_anverso, 'FCN_ID_EDOCTA')
        data_strings = df.rdd.map(format_row).collect()

        with open(f"gs://gestor-edo-cuenta/test_retiros/recaudacion_{term_id}.txt", "w") as f:
            f.write(data_strings)

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
