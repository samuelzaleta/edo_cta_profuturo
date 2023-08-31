from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import extract_terms, extract_dataset_spark
from profuturo.reporters import HtmlReporter
import sys


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])
area = int(sys.argv[4])
with define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term=term_id):
        # Extracción
        query = """
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               FTN_ID_MOV AS FCN_ID_MOVIMIENTO,
               FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               FCN_ID_SIEFORE,
               FTC_FOLIO,
               FTF_MONTO_ACCIONES,
               FTD_FEH_LIQUIDACION,
               FTF_MONTO_PESOS
        FROM CIERREN.TTAFOGRAL_MOV_CMS
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """
        truncate_table(postgres, "TTHECHOS_COMISION", term=term_id)
        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_spark,
            query,
            '"HECHOS"."TTHECHOS_COMISION"',
            term=term_id,
            params={"start": start_month, "end": end_month},
        )
        # Cifras de control
        report = html_reporter.generate(
            postgres,
            """
            SELECT I."FTC_GENERACION" AS GENERACION,
                   I."FTC_VIGENCIA" AS VIGENCIA,
                   I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                   I."FTC_ORIGEN" AS ORIGEN,
                   S."FTC_DESCRIPCION_CORTA" AS SIEFORE,
                   SUM(C."FTF_MONTO_PESOS") AS COMISIONES
            FROM "HECHOS"."TTHECHOS_COMISION" C
                INNER JOIN "TCHECHOS_CLIENTE" I ON C."FCN_CUENTA" = I."FCN_CUENTA" AND C."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
                INNER JOIN "TCDATMAE_SIEFORE" S ON C."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
            WHERE C."FCN_ID_PERIODO" = :term
            GROUP BY  I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN", S."FTC_DESCRIPCION_CORTA"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SIEFORE"],
            ["Comisiones"],
            params={"term": term_id},
        )

        notify(
            postgres,
            "Cifras de control Comisiones generadas",
            "Se han generado las cifras de control para comisiones exitosamente",
            report,
            term=term_id,
            control=True,
            area=area
        )
