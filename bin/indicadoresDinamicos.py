from sqlalchemy import text
from profuturo.common import define_extraction, truncate_table, notify,register_time
from profuturo.database import SparkConnectionConfigurator, get_postgres_pool, configure_mit_spark, configure_postgres_spark, configure_buc_spark, configure_integrity_spark, configure_bigquery_spark, get_bigquery_pool
from profuturo.extraction import update_indicator_spark, extract_dataset_spark, _get_spark_session, _write_spark_dataframe
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms, read_table_insert_temp_view
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
bigquery_pool = get_bigquery_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, bigquery_pool) as (postgres,bigquery):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session(excuetor_memory = '8g',
    memory_overhead ='1g',
    memory_offhead ='1g',
    driver_memory ='1g',
    intances = 4,
    parallelims = 8000)

    with register_time(postgres_pool, phase, term_id, user, area):
        # Indicadores dinámicos
        truncate_table(postgres, "TCHECHOS_CLIENTE_INDICADOR", term=term_id, area=area)

        all_user = """
        SELECT DISTINCT C."FTN_CUENTA" AS "FCN_CUENTA"
        FROM "MAESTROS"."TCDATMAE_CLIENTE" C
        """
        read_table_insert_temp_view(
            configure_postgres_spark,
            query=all_user,
            view="clientes"
        )

        postgres.execute(text("""
        UPDATE "HECHOS"."TCHECHOS_CLIENTE"
        SET "FTC_GENERACION" = 'DECIMO TRANSITORIO'
        WHERE "FCN_CUENTA" IN (SELECT "FCN_CUENTA" FROM "HECHOS"."TTHECHOS_CARGA_ARCHIVO" WHERE "FCN_ID_INDICADOR" = 50)
        AND "FCN_ID_PERIODO" = :term
                    """), {"term": term_id, "area": area})

        indicators = postgres.execute(text("""
        SELECT "FTN_ID_INDICADOR", "FTC_DESCRIPCION", "FTB_DISPONIBLE", "FTB_IMPRESION", "FTB_ENVIO", "FTB_GENERACION"
        FROM "GESTOR"."TCGESPRO_INDICADOR" I
        WHERE "FTB_ESTATUS" = true
        AND ARRAY[I."FTB_DISPONIBLE", I."FTB_ENVIO", I."FTB_IMPRESION", I."FTB_GENERACION"]  <> '{true,true,true,true}'
        """))

        for indicator in indicators.fetchall():
            print(f"Extracting {indicator[1]}...")

            indicators_queries = postgres.execute(text("""
                    SELECT "FTC_CONSULTA_SQL", "FTC_BD_ORIGEN"
                    FROM "TCGESPRO_INDICADOR_CONSULTA"
                    WHERE "FCN_ID_INDICADOR" = :indicator
                    """), {"indicator": indicator[0]})

            for indicator_query in indicators_queries.fetchall():
                query = indicator_query[0]

                view = indicator[0]

                origin = indicator_query[1]
                print(origin)

                origin_configurator: SparkConnectionConfigurator
                if origin == "BUC":
                    origin_configurator = configure_buc_spark
                elif origin == "MIT":
                    origin_configurator = configure_mit_spark
                elif origin == "INTGY":
                    origin_configurator = configure_integrity_spark('cierren')
                else:
                    origin_configurator = configure_postgres_spark

                update_indicator_spark(origin_configurator=origin_configurator, query=query, view=view,
                                       indicator= indicator._mapping,term=term_id,area= area,
                                       params={'term': term_id, 'end': end_month, 'start': start_month}
                                       )
                df =spark.sql(f"""
                SELECT
                DISTINCT 
                 CI.* FROM {view} CI 
                 INNER JOIN clientes C 
                 on CI.FCN_CUENTA = C.FCN_CUENTA
                 """)
                print(df.count())
                _write_spark_dataframe(df, configure_postgres_spark, '"HECHOS"."TCHECHOS_CLIENTE_INDICADOR"')
                spark.catalog.dropTempView(view)

            print(f"Done extracting {indicator[1]}!")

        # Indicadores manuales
        extract_dataset_spark(configure_postgres_spark, configure_postgres_spark, """
        SELECT
        "FCN_CUENTA",
        "FCN_ID_INDICADOR",
        CA."FCN_ID_AREA" AS "FCN_ID_AREA",
        ARRAY[I."FTB_DISPONIBLE", I."FTB_ENVIO", I."FTB_IMPRESION", I."FTB_GENERACION"] AS "FTA_EVALUA_INDICADOR"
        FROM
        "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA
            INNER JOIN
            "GESTOR"."TCGESPRO_INDICADOR" I ON CA."FCN_ID_INDICADOR" = I."FTN_ID_INDICADOR"
        WHERE
        "FCN_ID_PERIODO" = :term AND CA."FCN_ID_AREA" = :area AND
        ARRAY[I."FTB_DISPONIBLE", I."FTB_ENVIO", I."FTB_IMPRESION", I."FTB_GENERACION"]  <> '{true,true,true,true}'
        """, '"HECHOS"."TCHECHOS_CLIENTE_INDICADOR"', term=term_id, params={'term': term_id, 'area':area})

        truncate_table(postgres, 'TTEDOCTA_CLIENTE_INDICADOR', term=term_id)

        extract_dataset_spark(configure_postgres_spark, configure_postgres_spark, """
        SELECT 
        "FCN_CUENTA",
        "FCN_ID_PERIODO",
        :area AS "FCN_ID_AREA",
        MIN("FTB_ENVIO")::bool AS "FTB_ENVIO",
        MIN("FTB_IMPRESION")::bool AS "FTB_IMPRESION",
        MIN("FTB_DISPONIBLE")::bool AS "FTB_DISPONIBLE",
        MIN("FTB_GENERACION")::bool AS "FTB_GENERACION"
        FROM (
        SELECT 
        DISTINCT
        "FCN_CUENTA", "FCN_ID_PERIODO", "FCN_ID_INDICADOR",
        ("FTA_EVALUA_INDICADOR"[1])::int AS "FTB_DISPONIBLE",
        ("FTA_EVALUA_INDICADOR"[2])::int AS "FTB_ENVIO",
        ("FTA_EVALUA_INDICADOR"[3])::int AS "FTB_IMPRESION",
        ("FTA_EVALUA_INDICADOR"[4])::int AS "FTB_GENERACION"
        FROM "HECHOS"."TCHECHOS_CLIENTE_INDICADOR"
        WHERE "FCN_ID_PERIODO" = :term
        ) X
        GROUP BY
        "FCN_CUENTA",
        "FCN_ID_PERIODO"
        """, '"ESTADO_CUENTA"."TTEDOCTA_CLIENTE_INDICADOR"',params={'term': term_id,'area':area})


        query = """
        SELECT FTC_ENTIDAD_FEDERATIVA, INDICADOR, COUNT(1) NUM_REGISTROS
        FROM (
            SELECT COALESCE(MC."FTC_ENTIDAD_FEDERATIVA", 'NO ASIGNADO') AS FTC_ENTIDAD_FEDERATIVA,
                   GI."FTC_DESCRIPCION" AS INDICADOR 
            FROM "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" CI
                INNER JOIN "GESTOR"."TCGESPRO_INDICADOR" GI ON GI."FTN_ID_INDICADOR" = CI."FCN_ID_INDICADOR"
                LEFT JOIN "MAESTROS"."TCDATMAE_CLIENTE" MC ON CI."FCN_CUENTA" = MC."FTN_CUENTA"
            WHERE CI."FCN_ID_PERIODO" = :term
        ) I
        GROUP BY FTC_ENTIDAD_FEDERATIVA, INDICADOR
        """

        read_table_insert_temp_view(
            configure_postgres_spark,
            query,
            "INDICADOR",
            params={"term": term_id}
        )
        df = spark.sql("SELECT * FROM INDICADOR")

        # Convert PySpark DataFrame to pandas DataFrame
        pandas_df = df.toPandas()

        # Convert pandas DataFrame to HTML
        html_table = pandas_df.to_html()

        # Enviar notificación con la tabla HTML de este lote

        notify(
            postgres,
            f"Indicadores",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para indicadores exitosamente para el periodo",
            details=html_table,
            visualiza=False
        )

