from profuturo.common import register_time, define_extraction
from profuturo.database import get_postgres_pool
from profuturo.extraction import extract_terms
from profuturo.reporters import HtmlReporter
from pyspark.sql import SparkSession
import sys

spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('spark-bigquery-retiros') \
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


def format_data(row):
    return "|".join(row.asDict().values())


with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    retiros_general_columns = ["FCN_NUMERO_CUENTA", "FTC_NOMBRE", "FTC_CALLE_NUMERO", "FTC_COLONIA", "FTC_MUNICIPIO"
        , "FTC_CP", "FTC_ENTIDAD", "FTC_CURP", "FTC_RFC", "FTC_NSS"]
    retiros_columns = ["FCN_NUMERO_CUENTA", "FTD_FECHA_EMISION", "FTN_SDO_INI_AHO_RET", "FTN_SDO_INI_AHO_VIV",
                       "FTN_SDO_TRA_AHO_RET", "FTN_SDO_TRA_AHO_VIV", "FTN_SDO_REM_AHO_RET", "FTN_SDO_REM_AHO_VIV",
                       "FTC_LEY_PENSION", "FTC_REGIMEN", "FTC_SEGURO", "FTC_TIPO_PENSION", "FTC_FON_ENTIDAD",
                       "FTC_FON_NUMERO_POLIZA", "FTN_FON_MONTO_TRANSF", "FTD_FON_FECHA_TRANSF",
                       "TFN_FON_RETENCION_ISR", "FTC_AFO_ENTIDAD", "FTC_AFO_MEDIO_PAGO", "FTC_AFO_RECURSOS_ENTREGA",
                       "FTD_AFO_FECHA_ENTREGA", "FTC_AFO_RETENCION_ISR", "FTC_INFNVT_ENTIDAD",
                       "FTC_INFNVT_CTA_BANCARIA", "FTC_INFNVT_RECURSOS_ENTREGA", "FTC_INFNVT_FECHA_ENTREGA",
                       "FTC_INFNVT_RETENCION_ISR", "FTD_FECHA_EMISION", "FTD_FECHA_INICIO_PENSION",
                       "FTN_PENSION_INSTITUTO_SEG", "FTN_SALDO_FINAL"]
    retiros_general = extract_bigquery('ESTADO_CUENTA.TTMUESTR_RETIRO_GENERAL', term_id)
    retiros = extract_bigquery('ESTADO_CUENTA.TTMUESTR_RETIRO', term_id)

    df = (retiros_general.join(retiros, 'FCN_NUMERO_CUENTA')
          .select(retiros_general_columns[0], retiros_columns[1], retiros_general_columns[1:-1], retiros_general[2:-1]))
    df.show()

    df = df.rdd.map(format_data)
    df.write.text(f"gs://gestor-edo-cuenta/test_retiros/retiros_{term_id}.txt")
