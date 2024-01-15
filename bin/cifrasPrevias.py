from profuturo.common import register_time, define_extraction, notify, truncate_table
from profuturo.database import get_postgres_pool, get_buc_pool, configure_postgres_spark, configure_mit_spark,configure_bigquery_spark,get_bigquery_pool
from profuturo.extraction import _get_spark_session, _write_spark_dataframe, read_table_insert_temp_view, upsert_dataset,extract_dataset_spark
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
import sys
from datetime import datetime

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
bigquery_pool = get_bigquery_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])


with define_extraction(phase, area, postgres_pool, bigquery_pool) as (postgres, bigquery):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start = term["start_month"]
    end = term["end_month"]
    end_month_anterior = term["end_saldos_anterior"]
    valor_accion_anterior = term["valor_accion_anterior"]
    print(end_month_anterior, valor_accion_anterior)
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):

        truncate_table(bigquery, "ESTADO_CUENTA.TTEDOCTA_CLIENTE_INDICADOR", term=term_id, area=area)

        upsert_dataset(postgres, postgres, """
                SELECT
                "FCN_CUENTA" fcn_cuenta,
                CA."FCN_ID_AREA" as fcn_id_area,
                "FCN_ID_INDICADOR" fcn_id_indicador,
                ARRAY[I."FTB_DISPONIBLE", I."FTB_ENVIO", I."FTB_IMPRESION", I."FTB_GENERACION"] AS fta_evalua_inidcador
                FROM
                "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA
                    INNER JOIN
                    "GESTOR"."TCGESPRO_INDICADOR" I ON CA."FCN_ID_INDICADOR" = I."FTN_ID_INDICADOR"
                WHERE
                "FCN_ID_PERIODO" = :term AND CA."FCN_ID_AREA" = :area AND
                ARRAY[I."FTB_DISPONIBLE", I."FTB_ENVIO", I."FTB_IMPRESION", I."FTB_GENERACION"]  <> '{true,true,true,true}'
                """, """
                INSERT INTO "TCHECHOS_CLIENTE_INDICADOR" ("FCN_CUENTA", "FCN_ID_AREA","FCN_ID_PERIODO", "FCN_ID_INDICADOR", "FTA_EVALUA_INDICADOR")
                VALUES (...)
                ON CONFLICT ("FCN_CUENTA", "FCN_ID_PERIODO", "FCN_ID_INDICADOR") DO UPDATE 
                SET "FTA_EVALUA_INDICADOR" = EXCLUDED."FTA_EVALUA_INDICADOR"
                """, lambda i: [f":fcn_cuenta_{i}",f":fcn_id_area_{i}", f"{term_id}",f":fcn_id_indicador_{i}", f":fta_evalua_inidcador_{i}"],
                       "TCHECHOS_CLIENTE_INDICADOR", select_params ={'term':term_id, 'area':area})

        extract_dataset_spark(configure_postgres_spark, configure_bigquery_spark, """
                SELECT
                "FCN_CUENTA",
                "FCN_ID_PERIODO",
                :area AS "FCN_ID_AREA",
                MIN("FTB_ENVIO")::bool AS "FTB_ENVIO",
                MIN("FTB_IMPRESION")::bool AS "FTB_IMPRESION",
                MIN("FTB_DISPONIBLE")::bool AS "FTB_DISPONIBLE",
                MIN("FTB_GENERACION")::bool AS "FTB_GENERACION"
                FROM (
                SELECT "FCN_CUENTA", "FCN_ID_PERIODO", "FCN_ID_INDICADOR",
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
                """, "ESTADO_CUENTA.TTEDOCTA_CLIENTE_INDICADOR", params={'term': term_id, 'area': area})

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

        # Cifras de control
        report1 = html_reporter.generate(
            postgres,
            """
            SELECT I."FTC_GENERACION" AS GENERACION,
                   I."FTC_VIGENCIA" AS VIGENCIA,
                   I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                   I."FTC_ORIGEN" AS ORIGEN,
                   COUNT(DISTINCT I."FCN_CUENTA") AS CLIENTES,
                   coalesce(ROUND(cast(SUM(R."FTF_SALDO_INICIAL") AS numeric(16,2)) ,2),0) AS SALDO_INICIAL,
                   coalesce(ROUND(cast(SUM(R."FTF_SALDO_FINAL") AS numeric(16,2)) ,2),0) AS SALDO_FINAL,
                   ROUND(cast(SUM(R."FTF_ABONO") AS numeric(16,2)) ,2) AS ABONO,
                   ROUND(cast(SUM(R."FTF_CARGO") AS numeric(16,2)) ,2) AS CARGO,
                   coalesce(ROUND(cast(SUM(R."FTF_COMISION") AS numeric(16,2)) ,2),0) AS COMISION,
                   ROUND(cast(SUM(R."FTF_RENDIMIENTO_CALCULADO") AS numeric(16,2)) ,2) AS RENDIMIENTO
            FROM "HECHOS"."TTCALCUL_RENDIMIENTO" R
            INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON R."FCN_CUENTA" = I."FCN_CUENTA" AND R."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
            WHERE R."FCN_ID_PERIODO"
                    BETWEEN cast(TO_CHAR(DATE_TRUNC('MONTH', :start - INTERVAL '4 month'), 'YYYYMM') as int) and cast( TO_CHAR(DATE_TRUNC('MONTH', :end), 'YYYYMM') as int)
            GROUP BY I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Cliente", "Indicador Afiliación"],
            ["Clientes", "SALDO_INICIAL", "SALDO_FINAL", "ABONO", "CARGO", "COMISION", "RENDIMIENTO"],
            params={"term_id": term_id, "start":start, "end":end},
        )
        report2 = html_reporter.generate(
            postgres,
            """
            SELECT I."FTC_GENERACION" AS GENERACION,
                   I."FTC_VIGENCIA" AS VIGENCIA,
                   I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                   I."FTC_ORIGEN" AS ORIGEN,
                   S."FTC_DESCRIPCION_CORTA" AS SIEFORE,
                   mp."FTC_DESCRIPCION" AS CONCEPTO,
                   SUM(C."FTF_MONTO_PESOS") AS MONTO_PESOS
            FROM "HECHOS"."TTHECHOS_MOVIMIENTO" C
                INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON C."FCN_CUENTA" = I."FCN_CUENTA" AND C."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
                INNER JOIN "MAESTROS"."TCDATMAE_SIEFORE" S ON C."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
                INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" MP ON MP."FTN_ID_MOVIMIENTO_PROFUTURO" = C."FCN_ID_CONCEPTO_MOVIMIENTO"
            WHERE C."FCN_ID_PERIODO"
                    BETWEEN cast(TO_CHAR(DATE_TRUNC('MONTH', :start - INTERVAL '4 month'), 'YYYYMM') as int) and cast( TO_CHAR(DATE_TRUNC('MONTH', :end), 'YYYYMM') as int)
            GROUP BY  I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN", S."FTC_DESCRIPCION_CORTA", MP."FTC_DESCRIPCION"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SIEFORE", "Concepto"],
            ["MONTO_PESOS"],
            params={"term_id": term_id, "start": start, "end": end},
        )

        notify(
            postgres,
            f"CIFRAS PREVIAS",
            phase,
            area,
            term=term_id,
            message="Se han generado las cifras de previas exitosamente 1 de 2",
            details=report1,
        )
        notify(
            postgres,
            f"CIFRAS PREVIAS",
            phase,
            area,
            term=term_id,
            message="Se han generado las cifras de previas exitosamente 2 de 2",
            details=report2,
        )
