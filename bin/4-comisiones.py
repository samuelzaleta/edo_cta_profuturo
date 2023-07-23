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
        """, "TTHECHOS_COMISION", term=term_id, params={"start": date(2022, 9, 1), "end": date(2022, 9, 30)})

        # Cifras de control
        report = html_reporter.generate(
            postgres,
            """
            SELECT "FTO_INDICADORES"->>'34' AS generacion,
                   "FTO_INDICADORES"->>'21' AS vigencia,
                   CASE
                       WHEN "FTO_INDICADORES"->>'3' = 'Asignado' THEN 'Asignado'
                       WHEN "FTO_INDICADORES"->>'4' = 'Pensionado' THEN 'Pensionado'
                       WHEN "FTO_INDICADORES"->>'3' = 'Afiliado' THEN 'Afiliado'
                   END AS tipo_afiliado,
                   "FTO_INDICADORES"->>'33' AS tipo_cliente,
                   s."FTC_DESCRIPCION" AS siefore,
                   COUNT(*) AS clientes,
                   SUM("FTF_MONTO_PESOS") AS comisiones
            FROM "TTHECHOS_COMISION" mc
                LEFT JOIN "TCDATMAE_SIEFORE" s ON mc."FCN_ID_SIEFORE" = s."FTN_ID_SIEFORE"
                INNER JOIN "TCDATMAE_CLIENTE" c ON mc."FCN_CUENTA" = c."FTN_CUENTA"
                INNER JOIN "TCHECHOS_CLIENTE" i ON c."FTN_CUENTA" = i."FCN_CUENTA" AND i."FCN_ID_PERIODO" = :term
            GROUP BY "FTO_INDICADORES"->>'34',
                     "FTO_INDICADORES"->>'21',
                     CASE
                         WHEN "FTO_INDICADORES"->>'3' = 'Asignado' THEN 'Asignado'
                         WHEN "FTO_INDICADORES"->>'4' = 'Pensionado' THEN 'Pensionado'
                         WHEN "FTO_INDICADORES"->>'3' = 'Afiliado' THEN 'Afiliado'
                     END,
                     "FTO_INDICADORES"->>'33',
                     s."FTC_DESCRIPCION"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SIEFORE"],
            ["Registros", "Comisiones"],
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
