from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_postgres_oci_pool, configure_mit_spark, configure_postgres_oci_spark, configure_postgres_spark
from profuturo.extraction import extract_terms, extract_dataset_spark, read_table_insert_temp_view
from profuturo.reporters import HtmlReporter
from sqlalchemy import text
import sys
from datetime import datetime

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):
        # Extracción de tablas temporales
        query_temp = """
        SELECT
        "FTN_ID_TIPO_SUBCTA", "FCN_ID_REGIMEN", "FCN_ID_CAT_SUBCTA", "FCC_VALOR", "FTC_TIPO_CLIENTE"
        FROM "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"
        """
        extract_dataset_spark(
            configure_postgres_spark,
            configure_postgres_oci_spark,
            query_temp,
            '"MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"',
            term=term_id
        )

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
        truncate_table(postgres_oci, "TTHECHOS_COMISION", term=term_id)
        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_oci_spark,
            query,
            '"HECHOS"."TTHECHOS_COMISION"',
            term=term_id,
            params={"start": start_month, "end": end_month}
        )
        
        # Cifras de control
        report = html_reporter.generate(
            postgres_oci,
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
            f"Comisiones",
            phase,
            area,
            term=term_id,
            message=f"Se han ingestado las comisiones de forma exitosa para el periodo",
            details=report,
        )
        #Elimina tablas temporales
        postgres_oci.execute(text("""
        DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"
        """))
