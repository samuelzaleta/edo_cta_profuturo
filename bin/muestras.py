from sqlalchemy import text, Connection, CursorResult
from profuturo.database import  configure_postgres_spark, configure_mit_spark
from profuturo.extraction import _write_spark_dataframe, _get_spark_session, read_table_insert_temp_view
from profuturo.common import register_time, define_extraction,truncate_table
from profuturo.database import get_postgres_pool
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import lit, col, current_timestamp, concat
import sys


def find_samples(samplesCursor: CursorResult):
    samples = set()
    i = 0
    for batch in samplesCursor.partitions(50):
        for record in batch:
            account = record[0]
            consars = record[1]
            i = i + 1

            for consar in consars:
                if consar not in configurations:
                    continue

                configurations[consar] = configurations[consar] - 1

                if configurations[consar] == 0:
                    del configurations[consar]

                samples.add(account)

                if len(configurations) == 0:
                    print("Cantidad de registros: " + str(i))
                    return samples

    raise Exception('Insufficient samples')


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])
area = int(sys.argv[4])
user = int(sys.argv[3])
periodo = int(sys.argv[2])

with define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _):
    postgres: Connection

    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, area, usuario=user, term=term_id):
        truncate_table(postgres, "TCGESPRO_MUESTRA", term=term_id)
        # Extracción
        read_table_insert_temp_view(
            configure_postgres_spark,
            """
            SELECT 
            "FCN_CUENTA"
            FROM "HECHOS"."TTHECHOS_CARGA_ARCHIVO"
            WHERE "FTC_TRAMITE" = :tramite
            """,
            "muestrasManuales",
            params={"tramite":'MM'}
        )

        cursor = postgres.execute(text("""
        SELECT "FCN_ID_MOVIMIENTO_CONSAR", "FTN_CANTIDAD"
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_MUESTRA_AUTOMATICA"
        """))
        configurations = {configuration[0]: configuration[1] for configuration in cursor.fetchall()}
        print("configuracion",configurations)

        cursor = postgres.execute(text("""
        SELECT "FCN_CUENTA", array_agg(CC."FCN_ID_MOVIMIENTO_CONSAR")
        FROM (
            SELECT M."FCN_CUENTA", PC."FCN_ID_MOVIMIENTO_CONSAR"
            FROM "HECHOS"."TTHECHOS_MOVIMIENTO" M
                INNER JOIN "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON M."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
            WHERE M."FCN_ID_PERIODO" = :term
            GROUP BY M."FCN_CUENTA", PC."FCN_ID_MOVIMIENTO_CONSAR"
        ) AS CC
        INNER JOIN "GESTOR"."TTGESPRO_CONFIGURACION_MUESTRA_AUTOMATICA" MC ON CC."FCN_ID_MOVIMIENTO_CONSAR" = MC."FCN_ID_MOVIMIENTO_CONSAR"
        GROUP BY "FCN_CUENTA"
        ORDER BY sum(1.0 / "FTN_CANTIDAD") DESC
        """), {'term': term_id})
        samples = find_samples(cursor)

        print(samples)

        df = spark.sql(f"""
                        select 
                        * 
                        from 
                        muestrasManuales
                        """)
        record_list = df.collect()
        records_as_dicts = [record.FCN_CUENTA for record in record_list]
        records_as_dicts.extend(samples)
        print(records_as_dicts)
        filas = [{"FCN_CUENTA": records_as_dict} for records_as_dict in records_as_dicts]

        df = spark.createDataFrame(filas)
        df = df.withColumn("FCN_ID_PERIODO", lit(periodo))
        df = df.withColumn("FCN_ID_USUARIO", lit(user))
        df = df.withColumn("FCN_ID_AREA", lit(area))
        df = df.withColumn("FTC_ESTATUS", lit('Pendiente'))
        df = df.withColumn("FTD_FECHAHORA_ALTA", lit(current_timestamp()))
        df = df.withColumn("FTC_URL_PDF_ORIGEN",concat(lit("https://storage.cloud.google.com/gestor-edo-cuenta/estados_cuenta_archivos/"),col("FCN_CUENTA"),lit('_'),col("FCN_ID_PERIODO"), lit('_'),col("FCN_ID_USUARIO"),lit('.pdf')))
        _write_spark_dataframe(df, configure_postgres_spark, '"GESTOR"."TCGESPRO_MUESTRA"')