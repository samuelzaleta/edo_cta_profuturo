from profuturo.common import register_time, define_extraction, truncate_table, notify
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import _get_spark_session, _write_spark_dataframe, read_table_insert_temp_view
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    # Cifras de control
    report = html_reporter.generate(
        postgres,
        """
         SELECT I."FTC_GENERACION" AS GENERACION,
           I."FTC_VIGENCIA" AS VIGENCIA,
           I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
           I."FTC_ORIGEN" AS ORIGEN,
           COUNT(DISTINCT I."FCN_CUENTA") AS CLIENTES
    FROM "HECHOS"."TCHECHOS_CLIENTE" I
     WHERE I."FCN_ID_PERIODO" = 27
    GROUP BY I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN"
         """,
        ["Tipo Generación", "Vigencia", "Tipo Cliente", "Indicador Afiliación"],
        ["Clientes"],
        params={"term": term_id},
    )

    notify(
        postgres,
        "Clientes ingestados",
        f"Se han ingestado los clientes de forma exitosa para el periodo {time_period}",
        report,
        term=term_id,
    )

    # Cifras de control
    report1 = html_reporter.generate(
        postgres,
        """
        SELECT I."FTC_GENERACION" AS GENERACION,
               I."FTC_VIGENCIA" AS VIGENCIA,
               I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
               I."FTC_ORIGEN" AS ORIGEN,
               MC."FTC_DESCRIPCION" AS CONSAR,
               COUNT(DISTINCT M."FCN_CUENTA") AS CLIENTES,
               SUM(M."FTF_MONTO_PESOS") AS IMPORTE
        FROM "TTHECHOS_MOVIMIENTO" M
            INNER JOIN "TCHECHOS_CLIENTE" I ON M."FCN_CUENTA" = i."FCN_CUENTA" AND i."FCN_ID_PERIODO" = :term
            INNER JOIN "TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON M."FCN_ID_TIPO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
            INNER JOIN "TCDATMAE_MOVIMIENTO_CONSAR" MC ON PC."FCN_ID_MOVIMIENTO_CONSAR" = mc."FTN_ID_MOVIMIENTO_CONSAR"
        GROUP BY I."FTC_GENERACION" , I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN", MC."FTC_DESCRIPCION"
        """,
        ["Tipo Generación", "Vigencia", "Tipo Cliente", "Indicador Afiliación", "CONSAR"],
        ["Registros", "Importe"],
        params={"term": term_id},
    )

    report1 = html_reporter.generate(
        postgres,
        """
        SELECT
            I."FTC_GENERACION" AS GENERACION,
            I."FTC_VIGENCIA" AS VIGENCIA,
            I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
            I."FTC_ORIGEN" AS ORIGEN,
            TS."FCC_VALOR" AS TIPO_SUBCUENTA,
            S."FTC_DESCRIPCION_CORTA" AS SIEFORE,
            ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2) AS SALDO_INICIAL_PESOS,
            ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2)AS SALDO_FINAL_PESOS,
            ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTN_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_INICIAL_ACCIONES,
            ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTN_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_FINAL_ACCIONES
        FROM "HECHOS"."THHECHOS_SALDO_HISTORICO" SH
        INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON SH."FCN_CUENTA" = I."FCN_CUENTA"
        INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" TS ON SH."FCN_ID_TIPO_SUBCTA" = TS."FTN_ID_TIPO_SUBCTA"
        INNER JOIN "MAESTROS"."TCDATMAE_SIEFORE" S ON SH."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
        WHERE SH."FCN_ID_PERIODO" = :term and I."FCN_ID_PERIODO" = :term
        GROUP BY TS."FCC_VALOR", S."FTC_DESCRIPCION_CORTA",I."FTC_GENERACION" , I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN"
        """,
        ["Generación", "Vigencia", "tipo_cliente", "Origen", "Sub cuenta", "SIEFORE"],
        ["Saldo inicial en pesos", "Saldo final en pesos", "Saldo inicial en acciones", "Saldo final en acciones"],
        params={"term": term_id},
    )

    notify(
        postgres,
        "Cifras de control Saldos generadas 1 de 2",
        f"Se han generado las cifras de control para saldos exitosamente para el periodo {time_period}",
        report1,
        term=term_id,
        control=True,
    )

    # Cifras de control
    report1 = html_reporter.generate(
        postgres,
        """
        SELECT I."FTC_GENERACION" AS GENERACION,
               I."FTC_VIGENCIA" AS VIGENCIA,
               I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
               I."FTC_ORIGEN" AS ORIGEN,
               MC."FTC_DESCRIPCION" AS CONSAR,
               COUNT(DISTINCT M."FCN_CUENTA") AS CLIENTES,
               SUM(M."FTF_MONTO_PESOS") AS IMPORTE
        FROM "TTHECHOS_MOVIMIENTO" M
            INNER JOIN "TCHECHOS_CLIENTE" I ON M."FCN_CUENTA" = i."FCN_CUENTA" AND i."FCN_ID_PERIODO" = :term
            INNER JOIN "TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON M."FCN_ID_TIPO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
            INNER JOIN "TCDATMAE_MOVIMIENTO_CONSAR" MC ON PC."FCN_ID_MOVIMIENTO_CONSAR" = mc."FTN_ID_MOVIMIENTO_CONSAR"
        GROUP BY I."FTC_GENERACION" , I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN", MC."FTC_DESCRIPCION"
        """,
        ["Tipo Generación", "Vigencia", "Tipo Cliente", "Indicador Afiliación", "CONSAR"],
        ["Registros", "Importe"],
        params={"term": term_id},
    )
    notify(
        postgres,
        "Cifras de control movimientos generadas",
        f"Se han generado las cifras de control para comisiones exitosamente para el periodo {time_period}",
        report1,
        term=term_id,
        control=True,
    )