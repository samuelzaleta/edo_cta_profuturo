from profuturo.common import register_time, define_extraction, notify, truncate_table
from profuturo.database import get_postgres_pool, get_buc_pool, configure_postgres_spark, configure_mit_spark
from profuturo.extraction import _get_spark_session, _write_spark_dataframe, read_table_insert_temp_view
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
import sys
from datetime import datetime

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start = term["start_month"]
    end = term["end_month"]
    end_month_anterior = term["end_saldos_anterior"]
    valor_accion_anterior = term["valor_accion_anterior"]
    print(end_month_anterior, valor_accion_anterior)
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
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
            WHERE R."FCN_ID_PERIODO"
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
