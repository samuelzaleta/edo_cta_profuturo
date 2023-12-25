from profuturo.common import truncate_table, register_time, define_extraction, notify
from profuturo.database import get_postgres_pool, get_mit_pool, configure_postgres_spark, configure_mit_spark
from profuturo.extraction import extract_terms, _get_spark_session, read_table_insert_temp_view, _write_spark_dataframe
from profuturo.reporters import HtmlReporter
from pyspark.sql.functions import col, lit
from warnings import filterwarnings
import sys
from datetime import datetime

filterwarnings(action='ignore', category=DeprecationWarning, message='`np.bool` is a deprecated alias')
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = int(sys.argv[1])
area = int(sys.argv[4])
user = int(sys.argv[3])
print(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        truncate_table(postgres, 'TTHECHOS_RETIRO', term=term_id)
        #truncate_table(postgres, 'TTHECHOS_RETIRO_LIQUIDACIONES', term=term_id)
        #truncate_table(postgres, 'TTHECHOS_RETIRO_SALDOS_INICIALES', term=term_id)

        query_retiros = """
        WITH LIQ_SOLICITUDES AS (
        SELECT X.FTN_NUM_CTA_INVDUAL, X.FTC_FOLIO, X.FTC_FOLIO_REL, PT.TMC_DESC_ITGY,
               PT.TMC_DESC_NCI, PT.TMN_CVE_NCI, X.FCN_ID_PROCESO, X.FCN_ID_SUBPROCESO,
               X.FTD_FEH_CRE
        FROM (
            SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL, FCN_ID_PROCESO,
                   FCN_ID_SUBPROCESO, FTD_FEH_CRE
            FROM BENEFICIOS.THAFORETI_HIST_LIQ_SOLICITUDES ttls
            WHERE FTB_IND_FOLIO_AGRUP = '1'
              AND FCN_ID_ESTATUS = 6649
              -- AND tthls.FCN_ID_PROCESO IN (4045, 4046, 4047, 4048, 4049, 4050, 4051)
              AND FTD_FEH_CRE BETWEEN :start AND :end
              --AND  FTN_NUM_CTA_INVDUAL = 3200559346
            UNION ALL

            SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL,
                   FCN_ID_PROCESO, FCN_ID_SUBPROCESO, FTD_FEH_CRE
            FROM BENEFICIOS.TTAFORETI_LIQ_SOLICITUDES ttls
            WHERE FTB_IND_FOLIO_AGRUP = '1'
              AND FCN_ID_ESTATUS = 6649
              AND FTD_FEH_CRE BETWEEN :start AND :end
              --AND FTN_NUM_CTA_INVDUAL = 3200559346
        ) X
        INNER JOIN (
            SELECT distinct tms.TMC_DESC_ITGY,tms.TMC_DESC_NCI, tms.TMN_CVE_NCI, ttc.FCN_ID_SUBPROCESO
            FROM TMSISGRAL_MAP_NCI_ITGY tms
                INNER JOIN TTCRXGRAL_PAGO ttc ON tms.TMN_CVE_NCI = ttc.FCN_ID_SUBPROCESO
            WHERE tms.TMC_DESC_ITGY IN (
                'T73', 'TNP' ,'TPP', 'T97', 'TPR', 'TED', 'RJP', 'TRE', 'TJU', 'TEX', 'TGF', 'TPG', 'TRJ', 'TRU',
                'TIV', 'TIX', 'TEI', 'TPI', 'TNI', 'TJI', 'PPI', 'RCI', 'TAI'
            )
        ) PT ON PT.FCN_ID_SUBPROCESO = X.FCN_ID_SUBPROCESO
        ), RESOLUCIONES AS (
        SELECT
        resol.FTD_FEH_CRE, resol.FTD_FEH_INI_PEN,
        resol.FTD_FEH_EMI_RES,resol.FTC_CVE_TIPO_SEG,resol.FTC_SEC_PENSION,
        resol.FTC_CVE_REGIMEN,resol.FTC_TIPO_PRESTACION, resol.FTC_NSS, resol.FTC_CURP
        FROM TTAFORETI_RESOLUCIONES resol
        WHERE resol.FTD_FEH_INI_PEN IS NOT NULL
        AND (resol.FTD_FEH_CRE,resol.FTC_NSS) IN (SELECT MAX( resol.FTD_FEH_CRE),resol.FTC_NSS FROM TTAFORETI_RESOLUCIONES resol GROUP BY resol.FTC_NSS)
        )
        , DIS_TRANS AS (
        SELECT ROW_NUMBER() OVER (PARTITION BY FCN_CUENTA,FTC_FOLIO ORDER BY FTD_FEH_CRE DESC) rown,
               FCN_CUENTA, FTC_FOLIO, FTC_FOLIO_REL, FCN_ID_PROCESO, FCN_ID_SUBPROCESO,
               FTC_TPSEGURO, FTC_REGIMEN, FTC_TPPENSION, FTC_TMC_DESC_ITGY, FTC_TMC_DESC_NCI,
               FTN_TMN_CVE_NCI, FTN_FEH_INI_PEN, FTN_FEH_RES_PEN, FTD_FEH_CRE
        FROM (
                   SELECT L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, L.FTC_FOLIO AS FTC_FOLIO,
                   L.FTC_FOLIO_REL AS FTC_FOLIO_REL, L.FCN_ID_PROCESO AS FCN_ID_PROCESO,
                   L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO, T.FTC_TIPO_TRAMITE,
                   T.FTC_CVE_TIPO_SEG FTC_TPSEGURO, T.FTC_CVE_REGIMEN FTC_REGIMEN,
                   T.FTC_CVE_TIPO_PEN FTC_TPPENSION, L.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
                   L.TMC_DESC_NCI AS FTC_TMC_DESC_NCI, L.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
                   COALESCE(TO_CHAR(resol.FTD_FEH_INI_PEN,'YYYYMMDD'), T.FTC_FEH_INI_PEN) AS FTN_FEH_INI_PEN,
                   COALESCE(TO_CHAR(resol.FTD_FEH_EMI_RES,'YYYYMMDD'), T.FTC_FEH_RES_PEN) AS FTN_FEH_RES_PEN,
                   L.FTD_FEH_CRE
            FROM LIQ_SOLICITUDES L
                INNER JOIN BENEFICIOS.TTAFORETI_TRAMITE T ON L.FTC_FOLIO = T.FTC_FOLIO
                LEFT JOIN TTAFOGRAL_CTA_INVDUAL ctaind ON L.FTN_NUM_CTA_INVDUAL = ctaind.FTN_NUM_CTA_INVDUAL
                LEFT JOIN RESOLUCIONES resol ON ctaind.FTN_NSS =  resol.FTC_NSS
                --AND T.FTC_CVE_TIPO_SEG = resol.FTC_CVE_TIPO_SEG
                --AND T.FTC_CVE_TIPO_PEN = resol.FTC_SEC_PENSION
                --AND T.FTC_CVE_REGIMEN = resol.FTC_CVE_REGIMEN
                --AND T.FTC_TIPO_PRESTACION = resol.FTC_TIPO_PRESTACION
            WHERE L.TMC_DESC_ITGY IN ('T73', 'TNP', 'TPP', 'T97', 'TPR', 'TED', 'RJP', 'TRE', 'TJU', 'TEX', 'TGF', 'TPG', 'TRU', 'TIV')

            UNION ALL

            SELECT L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, L.FTC_FOLIO AS FTC_FOLIO,
                   L.FTC_FOLIO_REL AS FTC_FOLIO_REL, L.FCN_ID_PROCESO AS FCN_ID_PROCESO,
                   L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO, T.FTC_TIPO_TRAMITE,
                   T.FTC_CVE_TIPO_SEG FTC_TPSEGURO, T.FTC_CVE_REGIMEN FTC_REGIMEN,
                   T.FTC_CVE_TIPO_PEN FTC_TPPENSION, L.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
                   L.TMC_DESC_NCI AS FTC_TMC_DESC_NCI, L.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
                   COALESCE(T.FTC_FEH_INI_PEN,TO_CHAR(resol.FTD_FEH_INI_PEN,'YYYYMMDD')) AS FTN_FEH_INI_PEN,
                   COALESCE(T.FTC_FEH_RES_PEN, TO_CHAR(resol.FTD_FEH_EMI_RES,'YYYYMMDD')) AS FTN_FEH_RES_PEN,
                   L.FTD_FEH_CRE
            FROM LIQ_SOLICITUDES L
                INNER JOIN BENEFICIOS.TTAFORETI_TRAMITE T ON L.FTC_FOLIO = T.FTC_FOLIO
                LEFT JOIN TTAFOGRAL_CTA_INVDUAL ctaind ON T.FTN_NUM_CTA_INVDUAL = ctaind.FTN_NUM_CTA_INVDUAL
                LEFT JOIN RESOLUCIONES resol ON ctaind.FTC_CURP =  resol.FTC_CURP
                AND T.FTC_CVE_TIPO_SEG = resol.FTC_CVE_TIPO_SEG
                AND T.FTC_CVE_TIPO_PEN = resol.FTC_SEC_PENSION
                AND T.FTC_CVE_REGIMEN = resol.FTC_CVE_REGIMEN
                AND T.FTC_TIPO_PRESTACION = resol.FTC_TIPO_PRESTACION
            WHERE L.TMC_DESC_ITGY IN ('TIX', 'TEI', 'TPI', 'TNI', 'TJI', 'PPI', 'RCI', 'TAI')

            UNION ALL

            SELECT L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, L.FTC_FOLIO AS FTC_FOLIO,
                   L.FTC_FOLIO_REL, L.FCN_ID_PROCESO AS FCN_ID_PROCESO,
                   TR.FTN_ID_SUBPRO_TRAMITE AS FCN_ID_SUBPROCESO, TR.FTC_TIPO_TRAMITE,
                   TR.FCC_TPSEGURO AS FTC_TPSEGURO, TR.FTC_REGIMEN AS FTC_REGIMEN,
                   TR.FCC_TPPENSION AS FTC_TPPENSION, L.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
                   L.TMC_DESC_NCI AS FTC_TMC_DESC_NCI, L.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
                   COALESCE(TO_CHAR(FTD_FEC_INI_PENSION, 'YYYYMMDD'),TO_CHAR(resol.FTD_FEH_INI_PEN,'YYYYMMDD')) AS "FTN_FEH_INI_PEN",
                   COALESCE(TO_CHAR(FTD_FEC_EMI_RESOLUCION, 'YYYYMMDD'),TO_CHAR(resol.FTD_FEH_EMI_RES,'YYYYMMDD')) AS "FTN_FEH_RES_PEN",
                   L.FTD_FEH_CRE
            FROM LIQ_SOLICITUDES L
                INNER JOIN TTAFORETI_TRANS_RETI TR ON L.FTC_FOLIO = TR.FTC_FOLIO_SOLICITUD
                LEFT JOIN TTAFOGRAL_CTA_INVDUAL ctaind ON TR.FTN_NUM_CTA_INVDUAL = ctaind.FTN_NUM_CTA_INVDUAL
                LEFT JOIN TTAFORETI_RESOLUCIONES resol ON ctaind.FTN_NSS =  resol.FTC_NSS
                AND TR.FCC_TPSEGURO = resol.FTC_CVE_TIPO_SEG
                AND TR.FCC_TPPENSION = resol.FTC_SEC_PENSION
                AND TR.FTC_REGIMEN = resol.FTC_CVE_REGIMEN
                AND TR.FCC_TPPRESTACION = resol.FTC_TIPO_PRESTACION
            WHERE L.TMC_DESC_ITGY IN ('T73', 'TNP', 'TPP', 'T97', 'TPR', 'TED', 'RJP', 'TRE', 'TJU', 'TEX', 'TGF', 'TPG', 'TRU', 'TIV')

            UNION ALL

            SELECT L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, L.FTC_FOLIO AS FTC_FOLIO,
                   L.FTC_FOLIO_REL, L.FCN_ID_PROCESO AS FCN_ID_PROCESO,
                   TR.FTN_ID_SUBPRO_TRAMITE AS FCN_ID_SUBPROCESO, TR.FTC_TIPO_TRAMITE,
                   TR.FCC_TPSEGURO AS FTC_TPSEGURO, TR.FTC_REGIMEN AS FTC_REGIMEN,
                   TR.FCC_TPPENSION AS FTC_TPPENSION, L.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
                   L.TMC_DESC_NCI AS FTC_TMC_DESC_NCI, L.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
                   COALESCE(TO_CHAR(resol.FTD_FEH_INI_PEN,'YYYYMMDD'),TO_CHAR(FTD_FEC_INI_PENSION, 'YYYYMMDD')) AS "FTN_FEH_INI_PEN",
                   COALESCE(TO_CHAR(resol.FTD_FEH_EMI_RES,'YYYYMMDD'),TO_CHAR(FTD_FEC_EMI_RESOLUCION, 'YYYYMMDD')) AS "FTN_FEH_RES_PEN",
                   L.FTD_FEH_CRE
            FROM LIQ_SOLICITUDES L
                INNER JOIN TTAFORETI_TRANS_RETI TR ON L.FTC_FOLIO = TR.FTC_FOLIO_SOLICITUD
                LEFT JOIN TTAFOGRAL_CTA_INVDUAL ctaind ON TR.FTN_NUM_CTA_INVDUAL = ctaind.FTN_NUM_CTA_INVDUAL
                LEFT JOIN TTAFORETI_RESOLUCIONES resol ON ctaind.FTC_CURP =  resol.FTC_CURP
                AND TR.FCC_TPSEGURO = resol.FTC_CVE_TIPO_SEG
                AND TR.FCC_TPPENSION = resol.FTC_SEC_PENSION
                AND TR.FTC_REGIMEN = resol.FTC_CVE_REGIMEN
                AND TR.FCC_TPPRESTACION = resol.FTC_TIPO_PRESTACION
            WHERE L.TMC_DESC_ITGY IN ('TIX', 'TEI', 'TPI', 'TNI', 'TJI', 'PPI', 'RCI', 'TAI')

            UNION ALL

            SELECT L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, L.FTC_FOLIO AS FTC_FOLIO,
                   L.FTC_FOLIO_REL AS FTC_FOLIO_REL, TR.FTN_ID_SUBPRO_TRAMITE AS FCN_ID_PROCESO,
                   L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO, TR.FTC_TIPO_TRAMITE,
                   TR.FCC_TPSEGURO AS FTC_TPSEGURO, TR.FTC_REGIMEN AS FTC_REGIMEN,
                   TR.FCC_TPPENSION AS FTC_TPPENSION, L.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
                   L.TMC_DESC_NCI AS FTC_TMC_DESC_NCI, L.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
                   COALESCE(TO_CHAR(resol.FTD_FEH_INI_PEN,'YYYYMMDD'),TO_CHAR(FTD_FEC_INI_PENSION, 'YYYYMMDD')) AS "FTN_FEH_INI_PEN",
                   COALESCE(TO_CHAR(resol.FTD_FEH_EMI_RES,'YYYYMMDD'),TO_CHAR(FTD_FEC_EMI_RESOLUCION, 'YYYYMMDD')) AS "FTN_FEH_RES_PEN",
                   L.FTD_FEH_CRE
            FROM LIQ_SOLICITUDES L
                INNER JOIN THAFORETI_HIST_TRANS_RETI TR ON L.FTC_FOLIO = TR.FTC_FOLIO_SOLICITUD
                 LEFT JOIN TTAFOGRAL_CTA_INVDUAL ctaind ON TR.FTN_NUM_CTA_INVDUAL = ctaind.FTN_NUM_CTA_INVDUAL
                LEFT JOIN TTAFORETI_RESOLUCIONES resol ON ctaind.FTC_CURP =  resol.FTC_CURP
                AND TR.FCC_TPSEGURO = resol.FTC_CVE_TIPO_SEG
                AND TR.FCC_TPPENSION = resol.FTC_SEC_PENSION
                AND TR.FTC_REGIMEN = resol.FTC_CVE_REGIMEN
                AND TR.FCC_TPPRESTACION = resol.FTC_TIPO_PRESTACION
            WHERE L.TMC_DESC_ITGY IN ('TIX', 'TEI', 'TPI', 'TNI', 'TJI', 'PPI', 'RCI', 'TAI')

            UNION ALL

            SELECT L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, L.FTC_FOLIO AS FTC_FOLIO,
                   L.FTC_FOLIO_REL AS FTC_FOLIO_REL, TR.FTN_ID_SUBPRO_TRAMITE AS FCN_ID_PROCESO,
                   L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO, TR.FTC_TIPO_TRAMITE,
                   TR.FCC_TPSEGURO AS FTC_TPSEGURO, TR.FTC_REGIMEN AS FTC_REGIMEN,
                   TR.FCC_TPPENSION AS FTC_TPPENSION, L.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
                   L.TMC_DESC_NCI AS FTC_TMC_DESC_NCI, L.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
                   COALESCE(TO_CHAR(resol.FTD_FEH_INI_PEN,'YYYYMMDD'),TO_CHAR(FTD_FEC_INI_PENSION, 'YYYYMMDD')) AS "FTN_FEH_INI_PEN",
                   COALESCE(TO_CHAR(resol.FTD_FEH_EMI_RES,'YYYYMMDD'),TO_CHAR(FTD_FEC_EMI_RESOLUCION, 'YYYYMMDD')) AS "FTN_FEH_RES_PEN",
                   L.FTD_FEH_CRE
            FROM LIQ_SOLICITUDES L
                INNER JOIN THAFORETI_HIST_TRANS_RETI TR ON L.FTC_FOLIO = TR.FTC_FOLIO_SOLICITUD
                LEFT JOIN TTAFOGRAL_CTA_INVDUAL ctaind ON TR.FTN_NUM_CTA_INVDUAL = ctaind.FTN_NUM_CTA_INVDUAL
                LEFT JOIN TTAFORETI_RESOLUCIONES resol ON ctaind.FTN_NSS =  resol.FTC_NSS
                AND TR.FCC_TPSEGURO = resol.FTC_CVE_TIPO_SEG
                AND TR.FCC_TPPENSION = resol.FTC_SEC_PENSION
                AND TR.FTC_REGIMEN = resol.FTC_CVE_REGIMEN
                AND TR.FCC_TPPRESTACION = resol.FTC_TIPO_PRESTACION
            WHERE L.TMC_DESC_ITGY IN ('T73', 'TNP', 'TPP', 'T97', 'TPR', 'TED', 'RJP', 'TRE', 'TJU', 'TEX', 'TGF', 'TPG', 'TRU', 'TIV')
        ) X
        ), DATOS_PAGO AS (
        SELECT FTC_FOLIO,
               CASE
                   WHEN COUNT(DISTINCT FCC_VALOR) > 1 THEN 'MultiplesTiposDeBancos'
                   ELSE MAX(FCC_VALOR)
               END AS FCC_TIPO_BANCO,
               CASE
                   WHEN COUNT(DISTINCT FCC_DESC ) > 1 THEN 'MultiplesMedioPago'
                   ELSE MAX(FCC_DESC)
               END AS FCC_MEDIO_PAGO,
               SUM(FTN_ISR) as FTN_ISR
        FROM (
            SELECT ttcp.FTC_FOLIO, ttcp.FTN_ID_ASOCIADO, ttcp.FTN_ISR, thccc.FCC_VALOR,
                   thcccc.FCC_DESC, ttcp.FTN_NUM_REEXP
            FROM BENEFICIOS.TTCRXGRAL_PAGO ttcp
                INNER JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO thccc ON ttcp.FCC_CVE_BANCO = thccc.FCN_ID_CAT_CATALOGO
                INNER JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO thcccc ON ttcp.FCN_TIPO_PAGO = thcccc.FCN_ID_CAT_CATALOGO
            WHERE ttcp.FTD_FEH_CRE BETWEEN :start AND :end
              AND (FTC_FOLIO, FTN_ID_ASOCIADO, FTN_NUM_REEXP) IN (
                  SELECT FTC_FOLIO,FTN_ID_ASOCIADO, MAX(FTN_NUM_REEXP)
                  FROM BENEFICIOS.TTCRXGRAL_PAGO ttcp
                  GROUP BY FTC_FOLIO, FTN_ID_ASOCIADO
              )
        )
        GROUP BY FTC_FOLIO
        )
        SELECT X.FCN_CUENTA, X.FTC_FOLIO, X.FTC_FOLIO_REL, FCN_ID_PROCESO, FCN_ID_SUBPROCESO,
           FTC_TPSEGURO, FTC_REGIMEN, FTC_TPPENSION, coalesce(DP.FTN_ISR,0) AS FTN_ISR,
           DP.FCC_TIPO_BANCO, DP.FCC_MEDIO_PAGO, FTC_TMC_DESC_ITGY, FTC_TMC_DESC_NCI,
           FTN_TMN_CVE_NCI, coalesce(FTN_FEH_INI_PEN, '00010101') AS FTN_FEH_INI_PEN,
           coalesce(FTN_FEH_RES_PEN, '00010101') AS FTN_FEH_RES_PEN,
           CASE
               WHEN FTC_TMC_DESC_ITGY IN ('RJP', 'T73', 'TED', 'TGF', 'TJU', 'TPI', 'TPP', 'TPR', 'TRE', 'TRJ', 'TEX') THEN '73'
               WHEN FTC_TMC_DESC_ITGY IN ('PPI', 'RCI', 'T97', 'TAI', 'TEI', 'TIV', 'TIX', 'TJI', 'TNI', 'TNP', 'TPG') THEN '97'
           END FTN_ARCHIVO,
           CASE
               WHEN FTC_TMC_DESC_ITGY IN ('T73', 'TNP', 'TPP', 'T97', 'TPR', 'TED', 'RJP', 'TRE', 'TJU', 'TEX', 'TGF', 'TPG', 'TRU', 'TIV') THEN 'IMSS'
               WHEN FTC_TMC_DESC_ITGY IN ('TIX', 'TEI', 'TPI', 'TNI', 'TJI', 'PPI', 'RCI', 'TAI') THEN 'ISSSTE'
           END FTC_LEY_PENSION,
           CASE
               WHEN FTC_TMC_DESC_ITGY = 'TAI' THEN 'ASEGURADORA'
               WHEN FTC_TMC_DESC_ITGY IN ('TGF', 'TIV', 'TPG', 'TRJ') THEN 'GOBIERNO FEDERAL'
           END FTC_FON_ENTIDAD,
           FTD_FEH_CRE
        FROM (
        SELECT FCN_CUENTA, FTC_FOLIO, FTC_FOLIO_REL, FCN_ID_PROCESO, FCN_ID_SUBPROCESO,
               FTC_TPSEGURO, FTC_REGIMEN, FTC_TPPENSION, FTC_TMC_DESC_ITGY, FTC_TMC_DESC_NCI,
               FTN_TMN_CVE_NCI, FTN_FEH_INI_PEN, FTN_FEH_RES_PEN, FTD_FEH_CRE
        FROM DIS_TRANS d
        WHERE rown = 1
        ) X
        LEFT JOIN DATOS_PAGO DP ON DP.FTC_FOLIO = X.FTC_FOLIO
        """

        query_saldo_liquidaciones_sin_transferencias = """
        SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL,
        SUM( CASE WHEN FCN_ID_TIPO_SUBCTA IN (15, 16, 17, 18) THEN FTF_MONTO_PESOS ELSE 0 END ) AS SALDO_TRANSFERIDO_VIVIENDA,
        SUM( CASE WHEN FCN_ID_TIPO_SUBCTA NOT IN (15, 16, 17, 18) THEN FTF_MONTO_PESOS ELSE 0 END ) AS SALDO_TRANSFERIDO_AHORRORET
        FROM (
            SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA, FTF_MONTO_PESOS
            FROM CIERREN.TTAFOGRAL_MOV_RCV
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        
            UNION ALL
        
            SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA, FTF_MONTO_PESOS
            FROM CIERREN.TTAFOGRAL_MOV_GOB
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        
            UNION ALL
        
            SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA, FTF_MONTO_PESOS
            FROM CIERREN.TTAFOGRAL_MOV_VIV
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        
            UNION ALL
        
            SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA, FTF_MONTO_PESOS
            FROM CIERREN.TTAFOGRAL_MOV_COMP
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        
            UNION ALL
        
            SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA, FTF_MONTO_PESOS
            FROM CIERREN.TTAFOGRAL_MOV_SAR
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        
            UNION ALL
        
            SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA, FTF_MONTO_PESOS
            FROM CIERREN.TTAFOGRAL_MOV_AVOL
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        
            UNION ALL
        
            SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA, FTF_MONTO_PESOS
            FROM CIERREN.TTAFOGRAL_MOV_BONO
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        ) X
        GROUP BY FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL
        """

        query_saldo_liquidaciones_transferencias = """
        WITH LIQ_SOLICITUDES AS (
        SELECT X.FTN_NUM_CTA_INVDUAL, X.FTC_FOLIO, X.FTC_FOLIO_REL, PT.TMC_DESC_ITGY,
               PT.TMC_DESC_NCI, PT.TMN_CVE_NCI, X.FCN_ID_PROCESO, X.FCN_ID_SUBPROCESO,
               X.FTD_FEH_CRE
        FROM (
            SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL, FCN_ID_PROCESO,
                   FCN_ID_SUBPROCESO, FTD_FEH_CRE
            FROM BENEFICIOS.THAFORETI_HIST_LIQ_SOLICITUDES ttls
            WHERE FTB_IND_FOLIO_AGRUP = '1'
              AND FCN_ID_ESTATUS = 6649
              -- AND tthls.FCN_ID_PROCESO IN (4045, 4046, 4047, 4048, 4049, 4050, 4051)
              AND FTD_FEH_CRE BETWEEN :start AND :end
              --AND  FTN_NUM_CTA_INVDUAL = 3200559346
            UNION ALL
        
            SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL,
                   FCN_ID_PROCESO, FCN_ID_SUBPROCESO, FTD_FEH_CRE
            FROM BENEFICIOS.TTAFORETI_LIQ_SOLICITUDES ttls
            WHERE FTB_IND_FOLIO_AGRUP = '1'
              AND FCN_ID_ESTATUS = 6649
              AND FTD_FEH_CRE BETWEEN  :start AND :end
              --AND FTN_NUM_CTA_INVDUAL = 3200559346
        ) X
        INNER JOIN (
            SELECT distinct tms.TMC_DESC_ITGY,tms.TMC_DESC_NCI, tms.TMN_CVE_NCI, ttc.FCN_ID_SUBPROCESO
            FROM TMSISGRAL_MAP_NCI_ITGY tms
                INNER JOIN TTCRXGRAL_PAGO ttc ON tms.TMN_CVE_NCI = ttc.FCN_ID_SUBPROCESO
            WHERE tms.TMC_DESC_ITGY IN (
                'TGF','TPG','TRJ','TRU','TIV','TNP'
            )
        ) PT ON PT.FCN_ID_SUBPROCESO = X.FCN_ID_SUBPROCESO
        )
        SELECT PS.FTC_FOLIO,RET.FTC_FOLIO_REL, RET.FTN_NUM_CTA_INVDUAL,
               SUM(CASE WHEN PS.FCN_ID_TIPO_SUBCTA IN (15,16,17,18) THEN PS.FTN_MONTO_PESOS ELSE 0 END) SALDO_TRANSFERIDO_VIVIENDA,
               SUM(CASE WHEN PS.FCN_ID_TIPO_SUBCTA NOT IN (15,16,17,18) THEN PS.FTN_MONTO_PESOS ELSE 0 END) SALDO_TRANSFERIDO_AHORRORET
               FROM BENEFICIOS.TTCRXGRAL_PAGO_SUBCTA PS
                 INNER JOIN LIQ_SOLICITUDES RET
                 ON PS.FTC_FOLIO = RET.FTC_FOLIO
        WHERE (PS.FTC_FOLIO, PS.FCN_ID_TIPO_SUBCTA, PS.FTN_NUM_REEXP) IN (
                            SELECT PSMAX.FTC_FOLIO, PSMAX.FCN_ID_TIPO_SUBCTA,MAX(PSMAX.FTN_NUM_REEXP)FROM BENEFICIOS.TTCRXGRAL_PAGO_SUBCTA PSMAX
                            GROUP BY PSMAX.FTC_FOLIO, PSMAX.FCN_ID_TIPO_SUBCTA)
        --AND RET.FTN_NUM_CTA_INVDUAL = 3200559346
        GROUP BY PS.FTC_FOLIO, RET.FTN_NUM_CTA_INVDUAL,RET.FTC_FOLIO_REL
         """

        query_saldos ="""
        WITH RETIROS AS (
            SELECT X.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, X.FTC_FOLIO, X.FTC_FOLIO_REL,
                   PT.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY, PT.TMC_DESC_NCI, PT.TMN_CVE_NCI,
                   X.FCN_ID_PROCESO, X.FCN_ID_SUBPROCESO, X.FTD_FEH_CRE
            FROM (
                SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL, FCN_ID_PROCESO,
                       FCN_ID_SUBPROCESO, FTD_FEH_CRE
                FROM BENEFICIOS.THAFORETI_HIST_LIQ_SOLICITUDES
                WHERE FTB_IND_FOLIO_AGRUP = '1'
                  AND FCN_ID_ESTATUS = 6649
                  -- AND tthls.FCN_ID_PROCESO IN (4045, 4046, 4047, 4048, 4049, 4050, 4051)
                  AND FTD_FEH_CRE BETWEEN :start AND :end

                UNION ALL

                SELECT FTC_FOLIO, FTC_FOLIO_REL, FTN_NUM_CTA_INVDUAL,
                       FCN_ID_PROCESO, FCN_ID_SUBPROCESO, FTD_FEH_CRE
                FROM BENEFICIOS.TTAFORETI_LIQ_SOLICITUDES
                WHERE FTB_IND_FOLIO_AGRUP = '1'
                  AND FCN_ID_ESTATUS = 6649
                  AND FTD_FEH_CRE BETWEEN :start AND :end
            ) X
            INNER JOIN (
                SELECT distinct tms.TMC_DESC_ITGY,tms.TMC_DESC_NCI, tms.TMN_CVE_NCI, ttc.FCN_ID_SUBPROCESO
                FROM TMSISGRAL_MAP_NCI_ITGY tms
                    INNER JOIN TTCRXGRAL_PAGO ttc ON tms.TMN_CVE_NCI = ttc.FCN_ID_SUBPROCESO
                WHERE tms.TMC_DESC_ITGY IN (
                    'T73', 'TNP' ,'TPP', 'T97', 'TPR', 'TED', 'RJP', 'TRE', 'TJU', 'TEX', 'TGF', 'TPG', 'TRJ', 'TRU',
                    'TIV', 'TIX', 'TEI', 'TPI', 'TNI', 'TJI', 'PPI', 'RCI', 'TAI'
                )
            ) PT ON PT.FCN_ID_SUBPROCESO = X.FCN_ID_SUBPROCESO
        ), SALDOS_AL_DIA_LIQUIDACION AS (

            SELECT FCN_CUENTA, FTN_TIPO_AHORRO, FTC_TMC_DESC_ITGY, FTD_FEH_CRE,
                   SUM(FTF_SALDO_DIA) AS FTF_SALDO_DIA
            FROM (
                SELECT DISTINCT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, SH.FTD_FEH_LIQUIDACION,
                       RET.FTD_FEH_CRE, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, RET.FTC_TMC_DESC_ITGY,
                       CASE WHEN SH.FCN_ID_TIPO_SUBCTA IN (17, 18) THEN 1 ELSE 0 END FTN_TIPO_AHORRO,
                       VA.FCD_FEH_ACCION AS FCD_FEH_ACCION, VA.FCN_VALOR_ACCION AS VALOR_ACCION,
                       SH.FTN_DIA_ACCIONES AS FTF_DIA_ACCIONES,
                       ROUND(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION,2) AS FTF_SALDO_DIA
                FROM cierren.thafogral_saldo_historico_v2 SH
                    INNER JOIN RETIROS RET ON SH.FTN_NUM_CTA_INVDUAL = RET.FCN_CUENTA
                    INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
                    INNER JOIN (
                        SELECT DISTINCT SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_TIPO_SUBCTA, FTD_FEH_CRE,
                               RET.FTC_TMC_DESC_ITGY,SH.FCN_ID_SIEFORE,
                               MAX(TRUNC(SH.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
                        FROM CIERREN.THAFOGRAL_SALDO_HISTORICO_V2 SH
                            INNER JOIN RETIROS RET ON SH.FTN_NUM_CTA_INVDUAL = RET.FCN_CUENTA
                        WHERE SH.FTD_FEH_LIQUIDACION < RET.FTD_FEH_CRE
                          AND SH.FCN_ID_TIPO_SUBCTA NOT IN (15, 16)
                        GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_TIPO_SUBCTA, SH.FCN_ID_SIEFORE,
                                 RET.FTC_TMC_DESC_ITGY,
                                 FTD_FEH_CRE
                        ) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
                              AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA
                              AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
                              AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
                              AND RET.FTD_FEH_CRE = SHMAXIMO.FTD_FEH_CRE
                              AND RET.FTC_TMC_DESC_ITGY = SHMAXIMO.FTC_TMC_DESC_ITGY
                    INNER JOIN cierren.TCAFOGRAL_VALOR_ACCION VA
                        ON VA.FCD_FEH_ACCION = TO_DATE(TO_CHAR(RET.FTD_FEH_CRE, 'dd/MM/yyyy'), 'dd/MM/yyyy')
                       AND SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
            ) X
            GROUP BY FCN_CUENTA,FTN_TIPO_AHORRO,
                     FTC_TMC_DESC_ITGY, FTD_FEH_CRE
        ), SALDOS_INICIO_MES AS (
            SELECT FCN_CUENTA, FTN_TIPO_AHORRO, FTC_TMC_DESC_ITGY, FTD_FEH_CRE,
                   SUM(FTF_SALDO_DIA) AS FTF_SALDO_DIA
            FROM (
                SELECT DISTINCT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, SH.FTD_FEH_LIQUIDACION,
                       RET.FTD_FEH_CRE, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, RET.FTC_TMC_DESC_ITGY,
                       1 FTN_TIPO_AHORRO,
                       VA.FCD_FEH_ACCION AS FCD_FEH_ACCION, VA.FCN_VALOR_ACCION AS VALOR_ACCION,
                       SH.FTN_DIA_ACCIONES AS FTF_DIA_ACCIONES,
                       ROUND(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION,2) AS FTF_SALDO_DIA
                FROM cierren.thafogral_saldo_historico_v2 SH
                    INNER JOIN RETIROS RET ON SH.FTN_NUM_CTA_INVDUAL = RET.FCN_CUENTA
                    INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
                    INNER JOIN (
                        SELECT DISTINCT SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_TIPO_SUBCTA, FTD_FEH_CRE,
                               RET.FTC_TMC_DESC_ITGY,SH.FCN_ID_SIEFORE,
                               MAX(TRUNC(SH.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
                        FROM CIERREN.THAFOGRAL_SALDO_HISTORICO_V2 SH
                            INNER JOIN RETIROS RET ON SH.FTN_NUM_CTA_INVDUAL = RET.FCN_CUENTA
                        WHERE SH.FTD_FEH_LIQUIDACION < RET.FTD_FEH_CRE
                          AND SH.FCN_ID_TIPO_SUBCTA  IN (15, 16)
                        GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_TIPO_SUBCTA, SH.FCN_ID_SIEFORE,
                                 RET.FTC_TMC_DESC_ITGY,
                                 FTD_FEH_CRE
                        ) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
                              AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA
                              AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
                              AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
                              AND RET.FTD_FEH_CRE = SHMAXIMO.FTD_FEH_CRE
                              AND RET.FTC_TMC_DESC_ITGY = SHMAXIMO.FTC_TMC_DESC_ITGY
                    INNER JOIN cierren.TCAFOGRAL_VALOR_ACCION VA
                        ON VA.FCD_FEH_ACCION = :start
                       AND SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
            ) X
            GROUP BY FCN_CUENTA,FTN_TIPO_AHORRO,
                     FTC_TMC_DESC_ITGY, FTD_FEH_CRE
        ), SALDOS_CHEQUERA AS (
            SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, FTC_TMC_DESC_ITGY, FTN_TIPO_AHORRO, FTD_FEH_CRE,
                   SUM(PESOS) AS FTF_SALDO_DIA
            FROM (
                SELECT FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA, r.FTC_TMC_DESC_ITGY,
                       CASE WHEN FCN_ID_TIPO_SUBCTA IN (15, 16, 17, 18) THEN 1 ELSE 0 END FTN_TIPO_AHORRO,
                       FCN_ID_SIEFORE, r.FTD_FEH_CRE,SUM(FTN_DIA_PESOS) PESOS
                FROM TTAFOGRAL_BALANCE_MOVS_CHEQ q
                    INNER JOIN RETIROS r ON q.FTN_NUM_CTA_INVDUAL = r.FCN_CUENTA
                WHERE FTD_FEH_LIQUIDACION < :start
                  AND q.FCN_ID_SUBPROCESO NOT IN (10562,10573)
                 -- AND R.FTC_TMC_DESC_ITGY IN ('TJU', 'TGF', 'TPG', 'TRJ', 'TRU', 'TIV')
                GROUP BY FTN_NUM_CTA_INVDUAL, FCN_ID_TIPO_SUBCTA, FCN_ID_SIEFORE, r.FTC_TMC_DESC_ITGY, r.FTD_FEH_CRE
                HAVING SUM(FTN_DIA_PESOS) > 0
            ) X
            GROUP BY FTN_NUM_CTA_INVDUAL, FTC_TMC_DESC_ITGY, FTN_TIPO_AHORRO, FTD_FEH_CRE
        )
        SELECT FCN_CUENTA, FTC_TMC_DESC_ITGY, FTD_FEH_CRE,
        SUM(CASE WHEN FTN_TIPO_AHORRO = 1 THEN FTF_SALDO_DIA ELSE 0 END) AS FTN_SALDO_INI_VIVIENDA,
        SUM(CASE WHEN FTN_TIPO_AHORRO = 0 THEN FTF_SALDO_DIA ELSE 0 END) AS FTN_SALDO_INI_AHORRORET
        FROM (
            SELECT FCN_CUENTA, FTN_TIPO_AHORRO,  FTC_TMC_DESC_ITGY, FTD_FEH_CRE,
            FTF_SALDO_DIA
            FROM SALDOS_AL_DIA_LIQUIDACION

            UNION ALL

            SELECT FCN_CUENTA, FTN_TIPO_AHORRO, FTC_TMC_DESC_ITGY, FTD_FEH_CRE, FTF_SALDO_DIA
            FROM SALDOS_INICIO_MES

            UNION ALL

            SELECT FCN_CUENTA, FTN_TIPO_AHORRO, FTC_TMC_DESC_ITGY, FTD_FEH_CRE, FTF_SALDO_DIA
            FROM SALDOS_CHEQUERA
        ) X
        GROUP BY FCN_CUENTA, FTC_TMC_DESC_ITGY, FTD_FEH_CRE
        """

        read_table_insert_temp_view(
            configure_mit_spark,
            query_retiros,
            "RETIROS",
            params={"end": end_month, 'start': start_month, 'term': term_id}
        )

        read_table_insert_temp_view(
            configure_mit_spark,
            query_saldo_liquidaciones_sin_transferencias,
            "SALDOS_LIQUIDACIONES",
            params={"end": end_month, 'start': start_month, 'term': term_id}
        )

        read_table_insert_temp_view(
            configure_mit_spark,
            query_saldo_liquidaciones_transferencias,
            "SALDOS_LIQUIDACIONES_TRANSFERENCIAS",
            params={"end": end_month, 'start': start_month, 'term': term_id}
        )

        read_table_insert_temp_view(
            configure_mit_spark,
            query_saldos,
            "SALDOS_INICIALES",
            params={"end": end_month,'start': start_month, 'term': term_id}
        )

        df = spark.sql("""
        SELECT DISTINCT RET.FCN_CUENTA, RET.FTC_FOLIO, RET.FCN_ID_PROCESO,
               RET.FCN_ID_SUBPROCESO,SL.SALDO_TRANSFERIDO_VIVIENDA FTN_SALDO_TRANSFERIDO_VIVIENDA ,
               SL.SALDO_TRANSFERIDO_AHORRORET - RET.FTN_ISR AS FTN_SALDO_TRANSFERIDO_AHORRORET,
               RET.FTN_ISR AS FTN_ISR_LIQ_RET, 0 AS FTN_ISR_LIQ_VIV, 
               RET.FTC_TPSEGURO, RET.FTC_REGIMEN, RET.FTC_TPPENSION,
               RET.FCC_TIPO_BANCO, RET.FCC_MEDIO_PAGO, RET.FTC_TMC_DESC_ITGY,
               RET.FTC_TMC_DESC_NCI, RET.FTN_TMN_CVE_NCI, RET.FTN_FEH_INI_PEN, RET.FTN_FEH_RES_PEN,
               RET.FTN_ARCHIVO, RET.FTC_LEY_PENSION, RET.FTC_FON_ENTIDAD, RET.FTD_FEH_CRE
        FROM RETIROS RET
            INNER JOIN SALDOS_LIQUIDACIONES SL ON RET.FTC_FOLIO = SL.FTC_FOLIO AND RET.FTC_FOLIO_REL = SL.FTC_FOLIO_REL
        WHERE RET.FTC_TMC_DESC_ITGY IN (
            'T73' ,'TPP', 'T97', 'TPR', 'TED', 'RJP', 'TRE', 'TEX',
            'TIX', 'TEI', 'TPI', 'TNI', 'TJI', 'PPI', 'RCI', 'TAI'
        )

        UNION ALL

        SELECT DISTINCT RET.FCN_CUENTA, RET.FTC_FOLIO, RET.FCN_ID_PROCESO,
               RET.FCN_ID_SUBPROCESO, SL.SALDO_TRANSFERIDO_VIVIENDA  FTN_SALDO_TRANSFERIDO_VIVIENDA,
               CASE 
               WHEN RET.FTC_TMC_DESC_ITGY = 'TNP' THEN SL.SALDO_TRANSFERIDO_AHORRORET
                 ELSE SL.SALDO_TRANSFERIDO_AHORRORET - RET.FTN_ISR END FTN_SALDO_TRANSFERIDO_AHORRORET,
               RET.FTN_ISR AS FTN_ISR_LIQ_RET, 0 AS FTN_ISR_LIQ_VIV, 
               RET.FTC_TPSEGURO, RET.FTC_REGIMEN, RET.FTC_TPPENSION,
               RET.FCC_TIPO_BANCO, RET.FCC_MEDIO_PAGO, RET.FTC_TMC_DESC_ITGY,
               RET.FTC_TMC_DESC_NCI, RET.FTN_TMN_CVE_NCI, RET.FTN_FEH_INI_PEN, RET.FTN_FEH_RES_PEN,
               RET.FTN_ARCHIVO, RET.FTC_LEY_PENSION, RET.FTC_FON_ENTIDAD, RET.FTD_FEH_CRE
        FROM RETIROS RET
            INNER JOIN SALDOS_LIQUIDACIONES_TRANSFERENCIAS SL ON RET.FCN_CUENTA = SL.FTN_NUM_CTA_INVDUAL
        WHERE RET.FTC_TMC_DESC_ITGY IN ('TJU', 'TGF', 'TPG', 'TRJ', 'TRU','TIV','TNP')
        """)

        df.show()

        df = df.withColumn("FCN_ID_PERIODO", lit(term_id))

        df.createOrReplaceTempView("INFORMACIONRETIROS")

        #_write_spark_dataframe(df, configure_postgres_spark, '"HECHOS"."TTHECHOS_RETIRO_LIQUIDACIONES"')

        df = spark.sql("""
        SELECT 
        DISTINCT
        RL.FCN_CUENTA,
        RL.FTC_FOLIO,
        SL.FTN_SALDO_INI_AHORRORET as FTN_SDO_INI_AHORRORET,
        SL.FTN_SALDO_INI_VIVIENDA as FTN_SDO_INI_VIVIENDA,
        RL.FTN_SALDO_TRANSFERIDO_AHORRORET as FTN_SDO_TRA_AHORRORET,
        RL.FTN_SALDO_TRANSFERIDO_VIVIENDA as FTN_SDO_TRA_VIVIENDA,
        SL.FTN_SALDO_INI_AHORRORET - RL.FTN_SALDO_TRANSFERIDO_AHORRORET AS FTN_SDO_REM_AHORRORET,
        SL.FTN_SALDO_INI_VIVIENDA - RL.FTN_SALDO_TRANSFERIDO_VIVIENDA AS FTN_SDO_REM_VIVIENDA,
        RL.FTC_LEY_PENSION,
        RL.FTC_REGIMEN,
        RL.FTC_TPSEGURO,
        RL.FTC_TPPENSION,
        RL.FTC_FON_ENTIDAD,
        RL.FTD_FEH_CRE AS FTD_FECHA_EMISION,
        --0 AS FTN_RECURSO_RETENCION_ISR,
        RL.FCC_TIPO_BANCO AS FTC_ENT_REC_TRAN,
        RL.FCC_MEDIO_PAGO,
        --RL.FTN_SALDO_TRANSFERIDO_VIVIENDA + RL.FTN_SALDO_TRANSFERIDO_AHORRORET AS FTN_MONTO_TRANSFERIDO_AFORE,
        RL.FTN_ISR_LIQ_RET AS FTN_AFO_ISR,
        RL.FTN_FEH_INI_PEN,
        RL.FTN_FEH_RES_PEN,
        RL.FTC_TMC_DESC_ITGY AS FTC_TIPO_TRAMITE,
        RL.FTN_ARCHIVO,
        RL.FCN_ID_PERIODO
        FROM SALDOS_INICIALES SL
        INNER JOIN INFORMACIONRETIROS RL
         ON RL.FCN_CUENTA = SL.FCN_CUENTA  AND RL.FTC_TMC_DESC_ITGY = SL.FTC_TMC_DESC_ITGY
        AND SL.FTD_FEH_CRE = RL.FTD_FEH_CRE
        """)

        _write_spark_dataframe(df, configure_postgres_spark, '"HECHOS"."TTHECHOS_RETIRO"')

        query = """
                select
                R."FCN_CUENTA",
                "FTC_FOLIO",
                "FTN_SDO_INI_AHORRORET",
                "FTN_SDO_INI_VIVIENDA",
                "FTN_SDO_TRA_AHORRORET",
                "FTN_SDO_TRA_VIVIENDA",
                "FTN_SDO_INI_AHORRORET" - "FTN_SDO_TRA_AHORRORET" AS "FTN_SALDO_REM_AHORRORET",
                "FTN_SDO_INI_VIVIENDA" - "FTN_SDO_TRA_VIVIENDA" AS "FTN_SALDO_REM_VIVIENDA",
                "FTC_LEY_PENSION",
                "FTC_REGIMEN",
                "FTC_TPSEGURO",
                "FTC_TPPENSION",
                "FTC_FON_ENTIDAD",
                case
                when "FTC_FON_ENTIDAD" is not null then "FTN_SDO_TRA_VIVIENDA" + "FTN_SDO_TRA_AHORRORET"
                    else 0
                    end FTN_MONTO_TRANSFERIDO,
                TO_CHAR("FTD_FECHA_EMISION",'YYYYMMDD') AS "FTD_FECHA_EMISION",
                --0 AS FTN_RECURSO_RETENCION_ISR,
                "FTC_ENT_REC_TRAN",
                "FCC_MEDIO_PAGO",
                case
                when "FTC_FON_ENTIDAD" is null then "FTN_SDO_TRA_VIVIENDA" + "FTN_SDO_TRA_AHORRORET" - "FTN_AFO_ISR"
                    else 0
                    end "FTN_MONTO_TRANSFERIDO_AFORE",
                "FTN_AFO_ISR" AS "FTN_AFORE_RETENCION_ISR",
                "FTN_FEH_INI_PEN",
                "FTN_FEH_RES_PEN",
                "FTC_TIPO_TRAMITE",
                "FTN_ARCHIVO"
                from "HECHOS"."TTHECHOS_RETIRO" R
                where R."FCN_ID_PERIODO" = :term
                """

        read_table_insert_temp_view(configure_postgres_spark, query, "retiros",
                                    params={"term": term_id, "user": str(user), "area": area})
        df = spark.sql(""" select * from retiros""")
        # Convert PySpark DataFrame to pandas DataFrame
        pandas_df = df.toPandas()

        # Convert pandas DataFrame to HTML
        html_table = pandas_df.to_html()

        notify(
            postgres,
            f"retiros",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para retiros exitosamente para el periodo",
            details=html_table,
            visualiza=False
        )

