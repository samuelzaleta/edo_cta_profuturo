from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool, get_mit_url
from profuturo.extraction import extract_terms, extract_dataset_polars
from profuturo.reporters import HtmlReporter
from datetime import date
import sys


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term=term_id):
        # Extracción
        truncate_table(postgres, 'TTHECHOS_COMISION', term=term_id)
        extract_dataset_polars(get_mit_url(), postgres, """
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               FTN_ID_MOV AS FCN_ID_MOVIMIENTO,
               FCN_ID_SIEFORE,
               FTC_FOLIO,
               FTF_MONTO_ACCIONES,
               FTD_FEH_LIQUIDACION,
               FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_CMS
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "TTHECHOS_COMISION", term=term_id, params={"start": start_month, "end": end_month})

        # Cifras de control
        report = html_reporter.generate(
            postgres,
            """
            SELECT G."FTC_DESCRIPCION_CORTA" AS GENERACION,
                   I."FTC_VIGENCIA" AS VIGENCIA,
                   I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                   I."FTC_ORIGEN" AS ORIGEN,
                   S."FTC_DESCRIPCION_CORTA" AS SIEFORE,
                   SUM(C."FTF_MONTO_PESOS") AS COMISIONES
            FROM "TTHECHOS_COMISION" C
                INNER JOIN "TCHECHOS_CLIENTE" I ON C."FCN_CUENTA" = I."FCN_CUENTA" AND C."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
                INNER JOIN "TCGESPRO_GENERACION" G ON I."FTC_GENERACION" = G."FTN_ID_GENERACION"::varchar
                INNER JOIN "TCDATMAE_SIEFORE" S ON C."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
            WHERE C."FCN_ID_PERIODO" = :term
            GROUP BY G."FTC_DESCRIPCION_CORTA", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN", S."FTC_DESCRIPCION_CORTA"
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
        )
