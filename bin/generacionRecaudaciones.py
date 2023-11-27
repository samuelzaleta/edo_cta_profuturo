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
    df.createOrReplaceTempView('tabla')
    df = spark.sql("SELECT * FROM tabla")
    print(f"SE EXTRAIDO EXITOSAMENTE {table}")
    return df


def get_data(index, columns, df):
    if df.count() == 0:
        print("DATAFRAME VACIO")
    df = df.select(*columns)
    data = df.rdd.flatMap(lambda row: [f"{row[column]}" for column in columns]).collect()
    data = [list(data[i:i + len(columns)]) for i in range(0, len(data), len(columns))]
    res = f"{index}\n"
    for row in data:
        data_str = "|".join(row[i] for i in range(len(columns)))
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
                           "FTD_FECHA_CORTE", "FTB_PDF_IMPRESO", "FTN_SALDO_SUBTOTAL", "FTN_SALDO_TOTAL",
                           "FTN_PENSION_MENSUAL"]
        ahorro_columns = ["FTC_CONCEPTO_NEGOCIO", "FTN_SALDO_ANTERIOR", "FTF_APORTACION", "FTN_RETIRO",
                          "FTN_RENDIMIENTO", "FTN_COMISION", "FTN_SALDO_FINAL"]
        bono_columns = ["FTC_CONCEPTO_NEGOCIO", "FTN_VALOR_ACTUAL_UDI", "FTN_VALOR_NOMINAL_UDI",
                        "FTN_VALOR_ACTUAL_PESO", "FTN_VALOR_NOMINAL_PESO"]
        saldo_columns = ["FTC_GRUPO_CONCEPTO", "FTC_CONCEPTO_NEGOCIO", "FTN_SALDO_TOTAL"]
        cast_columns_general = ["FTN_SALDO_SUBTOTAL", "FTN_SALDO_TOTAL", "FTN_PENSION_MENSUAL"]
        cast_columns_anverso = ["FTF_APORTACION", "FTN_RETIRO", "FTN_RENDIMIENTO", "FTN_COMISION", "FTN_MONTO_PENSION",
                                "FTN_SALDO_ANTERIOR",
                                "FTN_SALDO_FINAL", "FTN_VALOR_ACTUAL_PESO", "FTN_VALOR_ACTUAL_UDI",
                                "FTN_VALOR_NOMINAL_PESO",
                                "FTN_VALOR_NOMINAL_UDI"]

        df_edo_general = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_GENERAL').filter(f"FCN_ID_PERIODO == {term_id}")
        df_anverso = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_ANVERSO')

        for cast_column in cast_columns_general:
            df_edo_general = df_edo_general.withColumn(f"{cast_column}", df_edo_general[f"{cast_column}"].cast("decimal(16, 2)"))
        for cast_columns in cast_columns_general:
            df_anverso = df_anverso.withColumn(f"{cast_column}", df_anverso[f"{cast_column}"].cast("decimal(16, 2)"))

        df_anverso_general = df_anverso.join(df_edo_general, "FCN_ID_EDOCTA")
        df_anverso_general = df_anverso_general.fillna(value="")
        df_anverso_aho = df_anverso_general.filter(f.col("FTC_SECCION").isin(["AHO", "PEN"]))
        df_anverso_bon = df_anverso_general.filter(f.col("FTC_SECCION") == "BON")
        df_anverso_sdo = df_anverso_general.filter(f.col("FTC_SECCION") == "SDO")
        df_edo_reverso = extract_bigquery('ESTADO_CUENTA.TTEDOCTA_REVERSO')

        clients = df_anverso_general.select("FCN_ID_EDOCTA").distinct()
        clients.show()
        clients = clients.toPandas().values.tolist()
        data_strings = ""
        general_count = 0
        ahorro_count = 0
        bono_count = 0
        saldo_count = 0

        for client in clients:
            print("\nGETTING DATA FROM: ", client, client[0], "\n")
            df_edo_general_temp = df_edo_general.filter(f.col("FCN_ID_EDOCTA") == client[0])
            df_anverso_aho_temp = df_anverso_aho.filter(f.col("FCN_ID_EDOCTA") == client[0])
            df_anverso_bon_temp = df_anverso_bon.filter(f.col("FCN_ID_EDOCTA") == client[0])
            df_anverso_sdo_temp = df_anverso_sdo.filter(f.col("FCN_ID_EDOCTA") == client[0])

            data_general, general_count = get_data(1, general_columns, df_edo_general_temp)
            data_ahorro, ahorro_count = get_data(2, ahorro_columns, df_anverso_aho_temp)
            data_bono, bono_count = get_data(3, bono_columns, df_anverso_bon_temp)
            data_saldo, saldo_count = get_data(4, saldo_columns, df_anverso_sdo_temp)

            if sum([general_count, ahorro_count, bono_count, saldo_count]) > 0:
                data_strings += data_general + data_ahorro + data_bono + data_saldo
                general_count += general_count
                ahorro_count += ahorro_count
                bono_count += bono_count
                saldo_count += saldo_count

        total_count = sum([general_count, ahorro_count, bono_count, saldo_count])
        final_row = f"5\n{general_count}|{ahorro_count}|{bono_count}|{saldo_count}|{total_count}"
        data_strings = data_strings + final_row
        str_to_gcs(data_strings, "recaudacion_anverso", term_id)

        reverso_columns = ["FCN_NUMERO_CUENTA", "FTN_ID_CONCEPTO", "FTC_SECCION", "FTD_FECHA_MOVIMIENTO",
                           "FTC_DESC_CONCEPTO", "FTC_PERIODO_REFERENCIA", "FTN_DIA_COTIZADO", "FTN_SALARIO_BASE",
                           "FTN_MONTO"]
        reverso_data, reverso_count = get_data(1, reverso_columns, df_edo_reverso)
        ret = df_edo_reverso.filter(f.col("FTC_SECCION") == "RET").count()
        vol = df_edo_reverso.filter(f.col("FTC_SECCION") == "VOL").count()
        viv = df_edo_reverso.filter(f.col("FTC_SECCION") == "VIV").count()
        total = df_edo_reverso.count()
        reverso_final_row = f"2\n{ret}|{vol}|{viv}|{total}"
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