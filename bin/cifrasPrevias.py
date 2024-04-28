from profuturo.common import register_time, define_extraction, notify, truncate_table
from profuturo.database import get_postgres_pool, get_postgres_oci_pool, configure_postgres_oci_spark, configure_mit_spark,configure_postgres_spark,get_bigquery_pool
from profuturo.extraction import _get_spark_session, _write_spark_dataframe, read_table_insert_temp_view, upsert_dataset,extract_dataset_spark, _create_spark_dataframe
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
import sys
import os
from datetime import datetime

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])
bucket_name = os.getenv("BUCKET_DEFINITIVO")
print(bucket_name)
prefix =f"{os.getenv('PREFIX_DEFINITIVO')}"
print(prefix)
url = os.getenv("URL_DEFINITIVO")
print(url)


with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start = term["start_month"]
    end = term["end_month"]
    end_month_anterior = term["end_saldos_anterior"]
    valor_accion_anterior = term["valor_accion_anterior"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    print(end_month_anterior, valor_accion_anterior)
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):

        try:
            truncate_table(postgres_oci, "ESTADO_CUENTA.TTEDOCTA_CLIENTE_INDICADOR", term=term_id, area=area)
        except:
            pass

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

        extract_dataset_spark(configure_postgres_spark, configure_postgres_oci_spark, """
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
                """, '"ESTADO_CUENTA"."TTEDOCTA_CLIENTE_INDICADOR"', params={'term': term_id, 'area': area})

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
        try:
            # Cifras de control
            report1 = html_reporter.generate(
                postgres_oci,
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
            report3 = html_reporter.generate(
                postgres,
                """
                SELECT C."FTC_ENTIDAD_FEDERATIVA",
                       sum(CASE WHEN IEI."FTB_ENVIO" THEN 1 ELSE 0 END) AS impresion,
                       sum(CASE WHEN NOT IEI."FTB_ENVIO" AND IEI."FTB_IMPRESION" THEN 1 ELSE 0 END) AS electronico,
                       sum(CASE WHEN IEI."FTB_ENVIO" OR IEI."FTB_IMPRESION" THEN 1 ELSE 0 END) AS total,
                       sum(CASE WHEN IAC."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) AS aclaracion_cuentas,
                       sum(CASE WHEN IAV."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) AS aportaciones_voluntarias,
                       0 /* sum(CASE WHEN IDM."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) */ AS devolucion_mensajeria,
                       sum(CASE WHEN IMBO."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) AS marca_bloqueo_operativo,
                       sum(CASE WHEN IMCF."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) AS marca_correo_fisico,
                       sum(CASE WHEN IMCEI."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) AS marca_correo_electronico_invalido,
                       sum(CASE WHEN IMCSTI."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) AS marca_correo_sms_telefono_invalido,
                       sum(CASE WHEN IMDCF."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) AS marca_devoluciones_correo_fisico,
                       sum(CASE WHEN IRC."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) AS recaudacion,
                       sum(CASE WHEN IRT."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) AS retiros,
                       sum(CASE WHEN ISC."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) AS servicio_clientes,
                       sum(CASE WHEN IDI."FCN_CUENTA" IS NOT NULL THEN 1 ELSE 0 END) AS direccion_invalida
                FROM "MAESTROS"."TCDATMAE_CLIENTE" C
                    LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" IAC ON C."FTN_CUENTA" = IAC."FCN_CUENTA" AND IAC."FCN_ID_PERIODO" = :term AND IAC."FCN_ID_INDICADOR" = 47
                    LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" IAV ON C."FTN_CUENTA" = IAV."FCN_CUENTA" AND IAV."FCN_ID_PERIODO" = :term AND IAV."FCN_ID_INDICADOR" = 46
                    -- LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" IDM ON C."FTN_CUENTA" = IDM."FCN_CUENTA" AND IDM."FCN_ID_PERIODO" = :term AND IDM."FCN_ID_INDICADOR" = ?
                    LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" IMBO ON C."FTN_CUENTA" = IMBO."FCN_CUENTA" AND IMBO."FCN_ID_PERIODO" = :term AND IMBO."FCN_ID_INDICADOR" = 17
                    LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" IMCF ON C."FTN_CUENTA" = IMCF."FCN_CUENTA" AND IMCF."FCN_ID_PERIODO" = :term AND IMCF."FCN_ID_INDICADOR" = 27
                    LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" IMCEI ON C."FTN_CUENTA" = IMCEI."FCN_CUENTA" AND IMCEI."FCN_ID_PERIODO" = :term AND IMCEI."FCN_ID_INDICADOR" = 25
                    LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" IMCSTI ON C."FTN_CUENTA" = IMCSTI."FCN_CUENTA" AND IMCSTI."FCN_ID_PERIODO" = :term AND IMCSTI."FCN_ID_INDICADOR" = 26
                    LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" IMDCF ON C."FTN_CUENTA" = IMDCF."FCN_CUENTA" AND IMDCF."FCN_ID_PERIODO" = :term AND IMDCF."FCN_ID_INDICADOR" = 23
                    LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" IRC ON C."FTN_CUENTA" = IRC."FCN_CUENTA" AND IRC."FCN_ID_PERIODO" = :term AND IRC."FCN_ID_INDICADOR" = 45
                    LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" IRT ON C."FTN_CUENTA" = IRT."FCN_CUENTA" AND IRT."FCN_ID_PERIODO" = :term AND IRT."FCN_ID_INDICADOR" = 48
                    LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" ISC ON C."FTN_CUENTA" = ISC."FCN_CUENTA" AND ISC."FCN_ID_PERIODO" = :term AND ISC."FCN_ID_INDICADOR" = 22
                    LEFT OUTER JOIN "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" IDI ON C."FTN_CUENTA" = IDI."FCN_CUENTA" AND IDI."FCN_ID_PERIODO" = :term AND IDI."FCN_ID_INDICADOR" = 16
                    LEFT OUTER JOIN (
                        SELECT "FCN_CUENTA",
                               bool_and("FTA_EVALUA_INDICADOR"[2]) AS "FTB_ENVIO",
                               bool_and("FTA_EVALUA_INDICADOR"[3]) AS "FTB_IMPRESION"
                        FROM "HECHOS"."TCHECHOS_CLIENTE_INDICADOR"
                        WHERE "FCN_ID_PERIODO" = :term
                        GROUP BY "FCN_CUENTA"
                    ) IEI ON C."FTN_CUENTA" = IEI."FCN_CUENTA"
                GROUP BY "FTC_ENTIDAD_FEDERATIVA"
                """,
                ["Entidad"],
                [
                    "Impresión", "Electrónico", "Total", "Aclaración de cuentas", "Aportaciones voluntarias",
                    "Devolución Mensajería", "Marca bloqueo operativo", "Marca Correo Fisico",
                    "Marca de correo electrónico invalido", "Marca de correo SMS -Telefono Invalido",
                    "Marca devoluciones de correo Fisico", "Recaudación", "Retiros", "Servicio a clientes",
                    "Dirección Invalida Automático", "Total Indicadores"
                ],
                params={"term": term_id},
            )
        except:
            pass

        notify(
            postgres,
            f"CIFRAS PREVIAS",
            phase,
            area,
            term=term_id,
            message="Se han generado las cifras de previas exitosamente 1 de 2",
            #details=report1,
        )
        notify(
            postgres,
            f"CIFRAS PREVIAS",
            phase,
            area,
            term=term_id,
            message="Se han generado las cifras de previas exitosamente 2 de 2",
            #details=report2,
        )
        notify(
            postgres,
            f"Cifras Correspondencia",
            phase,
            area,
            term=term_id,
            message="Se han generado las cifras de correspondencia exitosamente",
            #details=report3,
        )
