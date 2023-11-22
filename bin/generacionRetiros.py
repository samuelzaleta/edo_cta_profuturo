from profuturo.common import register_time, define_extraction, notify
from profuturo.database import get_postgres_pool
from profuturo.extraction import extract_terms
from profuturo.reporters import HtmlReporter
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
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
        .option('table', f'estado-de-cuenta-service-dev-b:{table}') \
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

    with register_time(postgres_pool, phase, term_id, user, area):

        retiros_general_columns = ["FCN_NUMERO_CUENTA", "FTC_NOMBRE", "FTC_CALLE_NUMERO", "FTC_COLONIA", "FTC_MUNICIPIO"
            , "FTC_CP", "FTC_ENTIDAD", "FTC_CURP", "FTC_RFC", "FTC_NSS"]
        retiros_columns = ["FCN_NUMERO_CUENTA", "FTD_FECHA_EMISION", "FTN_SDO_INI_AHO_RET", "FTN_SDO_INI_AHO_VIV",
                           "FTN_SDO_TRA_AHO_RET", "FTN_SDO_TRA_AHO_VIV", "FTN_SDO_REM_AHO_RET", "FTN_SDO_REM_AHO_VIV",
                           "FTC_LEY_PENSION", "FTC_REGIMEN", "FTC_SEGURO", "FTC_TIPO_PENSION", "FTC_FON_ENTIDAD",
                           "FTC_FON_NUMERO_POLIZA", "FTN_FON_MONTO_TRANSF", "FTD_FON_FECHA_TRANSF",
                           "TFN_FON_RETENCION_ISR", "FTC_AFO_ENTIDAD", "FTC_AFO_MEDIO_PAGO", "FTC_AFO_RECURSOS_ENTREGA",
                           "FTD_AFO_FECHA_ENTREGA", "FTC_AFO_RETENCION_ISR", "FTC_INFNVT_ENTIDAD",
                           "FTC_INFNVT_CTA_BANCARIA", "FTC_INFNVT_RECURSOS_ENTREGA", "FTC_INFNVT_FECHA_ENTREGA",
                           "FTC_INFNVT_RETENCION_ISR", "FTD_FECHA_INICIO_PENSION",
                           "FTN_PENSION_INSTITUTO_SEG", "FTN_SALDO_FINAL"]
        retiros_general = extract_bigquery('ESTADO_CUENTA.TTMUESTR_RETIRO_GENERAL', term_id).select(*retiros_general_columns)
        retiros = extract_bigquery('ESTADO_CUENTA.TTMUESTR_RETIRO', term_id).select(*retiros_columns)
        columns = [retiros_general_columns[0]] + [retiros_columns[1]] + retiros_general_columns[1:] + retiros_general_columns[2:]
        df = retiros_general.join(retiros, 'FCN_NUMERO_CUENTA').drop(retiros['FCN_NUMERO_CUENTA'])
        df_fecha_emision = df.select("FTD_FECHA_EMISION")
        left_columns = df.select("FTD_FECHA_INICIO_PENSION", "FTN_PENSION_INSTITUTO_SEG", "FTN_SALDO_FINAL")
        df.printSchema()
        df = df.drop("FTD_FECHA_INICIO_PENSION", "FTN_PENSION_INSTITUTO_SEG", "FTN_SALDO_FINAL")
        df = df.withColumn("FTD_FECHA_EMISION_2", f.lit(df_fecha_emision["FTD_FECHA_EMISION"]))
        df.printSchema()
        left_columns.printSchema()
        df = df.withColumn("FTD_FECHA_INICIO_PENSION", f.lit(left_columns["FTD_FECHA_INICIO_PENSION"]))
        df = df.withColumn("FTN_PENSION_INSTITUTO_SEG", f.lit(left_columns["FTN_PENSION_INSTITUTO_SEG"]))
        df = df.withColumn("FTN_SALDO_FINAL", f.lit(left_columns["FTN_SALDO_FINAL"]))
        df.show()
        df.printSchema()

        df.write.csv(f"gs://edo_cuenta_profuturo_dev_b/test_retiros/retiros_{term_id}.txt", sep="|")
        """
        notify(
            postgres,
            f"Generacion de archivos Retiros",
            phase,
            area,
            term=term_id,
            message=f"Se han exportado retiros para el periodo {time_period}",
            aprobar=False,
            descarga=False
        )"""