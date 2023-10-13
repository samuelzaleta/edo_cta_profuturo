from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import extract_terms, extract_dataset_spark
from profuturo.reporters import HtmlReporter
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
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):
        # Extracción
        query = """
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               C.FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               C.FTN_ID_MOV AS FCN_ID_MOVIMIENTO,
               C.FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               C.FCN_ID_SIEFORE,
               C.FTC_FOLIO,
               C.FTF_MONTO_ACCIONES,
               C.FTD_FEH_LIQUIDACION,
               C.FTF_MONTO_PESOS,
               S.FCN_ID_TIPO_SUBCTA as FTN_TIPO_SUBCTA
        FROM CIERREN.TTAFOGRAL_MOV_CMS C
        INNER JOIN CIERREN.TFCRXGRAL_CONFIG_MOV_ITGY M
        ON C.FCN_ID_CONCEPTO_MOV =M.FFN_ID_CONCEPTO_MOV
        INNER JOIN TRAFOGRAL_MOV_SUBCTA S ON M.FRN_ID_MOV_SUBCTA = S.FRN_ID_MOV_SUBCTA
        WHERE C.FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """
        truncate_table(postgres, "TTHECHOS_COMISION", term=term_id)
        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_spark,
            query,
            '"HECHOS"."TTHECHOS_COMISION"',
            term=term_id,
            params={"start": start_month, "end": end_month}
        )
        # Cifras de control
        report = html_reporter.generate(
            postgres,
            """
            SELECT I."FTC_GENERACION" AS GENERACION,
                   I."FTC_VIGENCIA" AS VIGENCIA,
                   I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                   I."FTC_ORIGEN" AS ORIGEN,
                   S."FCC_VALOR" AS SUBCUENTA,
                   ROUND(SUM(C."FTF_MONTO_PESOS")::numeric, 2) AS COMISIONES
            FROM "HECHOS"."TTHECHOS_COMISION" C
                INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON C."FCN_CUENTA" = I."FCN_CUENTA" AND C."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
                INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" S ON C."FTN_TIPO_SUBCTA" = S."FTN_ID_TIPO_SUBCTA"
            WHERE C."FCN_ID_PERIODO" = :term
            GROUP BY  I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN", S."FCC_VALOR"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SUBCUENTA"],
            ["Monto_Comisiones"],
            params={"term": term_id},
        )

        notify(
            postgres,
            f"Cifras de control Comisiones generadas",
            phase,
            area,
            term=term_id,
            message=f"Se han ingestado las comisiones de forma exitosa para el periodo {time_period}",
            details=report,
        )
