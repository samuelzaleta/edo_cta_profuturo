from profuturo.common import truncate_table, register_time, define_extraction, notify
from profuturo.database import get_postgres_pool, get_mit_pool, configure_postgres_spark, configure_mit_spark
from profuturo.extraction import extract_terms, _get_spark_session, read_table_insert_temp_view, extract_dataset_spark
from profuturo.reporters import HtmlReporter
from pyspark.sql.functions import col
from warnings import filterwarnings
import sys
from datetime import datetime

filterwarnings(action='ignore', category=DeprecationWarning, message='`np.bool` is a deprecated alias')
html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = int(sys.argv[1])
area = int(sys.argv[4])
user = int(sys.argv[3])
print(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, area, usuario=user, term=term_id):
        truncate_table(postgres, 'TTHECHOS_RETIRO', term=term_id)
        query_liquidaciones = """
            WITH LIQ_SOLICITUDES AS (
            SELECT
            tthls.FTC_FOLIO,
            tthls.FTC_FOLIO_REL,
            tthls.FTN_NUM_CTA_INVDUAL,
            tthls.FCN_ID_PROCESO,
            tthls.FCN_ID_SUBPROCESO,
            tthls.FCN_ID_ESTATUS,
            tthls.FTB_IND_FOLIO_AGRUP,
            tthls.FTC_NSS,
            tthls.FTC_CURP,
            tthls.FTD_FEH_CRE, -- CONDICION
            tthls.FTC_USU_CRE,
            tthls.FTD_FEH_ACT,
            tthls.FTC_USU_ACT
            FROM BENEFICIOS.THAFORETI_HIST_LIQ_SOLICITUDES tthls
            WHERE tthls.FTB_IND_FOLIO_AGRUP = '1'
            AND tthls.FCN_ID_ESTATUS = 6649
            AND tthls.FCN_ID_PROCESO IN (4045, 4046, 4047, 4048, 4049, 4050, 4051)
            AND tthls.FTD_FEH_CRE BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy') --:start AND :end
            union all
            SELECT ttls.FTC_FOLIO,
            ttls.FTC_FOLIO_REL,
            ttls.FTN_NUM_CTA_INVDUAL,
            ttls.FCN_ID_PROCESO,
            ttls.FCN_ID_SUBPROCESO,
            ttls.FCN_ID_ESTATUS,
            ttls.FTB_IND_FOLIO_AGRUP,
            ttls.FTC_NSS,
            ttls.FTC_CURP,
            ttls.FTD_FEH_CRE, -- CONDICION
            ttls.FTC_USU_CRE,
            ttls.FTD_FEH_ACT,
            ttls.FTC_USU_ACT
            FROM BENEFICIOS.TTAFORETI_LIQ_SOLICITUDES ttls
            WHERE ttls.FTB_IND_FOLIO_AGRUP = '1'
            AND ttls.FCN_ID_ESTATUS = 6649
            AND ttls.FCN_ID_PROCESO IN (4045, 4046, 4047, 4048, 4049, 4050, 4051)
            AND ttls.FTD_FEH_CRE BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy')
            )
            , LIQ AS (
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            ls.FCN_ID_PROCESO,
            ls.FCN_ID_SUBPROCESO,
            ls.FCN_ID_ESTATUS,
            ls.FTB_IND_FOLIO_AGRUP,
            ls.FTC_NSS,
            ls.FTC_CURP,
            ls.FTD_FEH_CRE, -- CONDICION
            ls.FTC_USU_CRE,
            ls.FTD_FEH_ACT,
            ls.FTC_USU_ACT,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_RCV DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            ls.FCN_ID_PROCESO,
            ls.FCN_ID_SUBPROCESO,
            ls.FCN_ID_ESTATUS,
            ls.FTB_IND_FOLIO_AGRUP,
            ls.FTC_NSS,
            ls.FTC_CURP,
            ls.FTD_FEH_CRE, -- CONDICION
            ls.FTC_USU_CRE,
            ls.FTD_FEH_ACT,
            ls.FTC_USU_ACT,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_GOB DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            ls.FCN_ID_PROCESO,
            ls.FCN_ID_SUBPROCESO,
            ls.FCN_ID_ESTATUS,
            ls.FTB_IND_FOLIO_AGRUP,
            ls.FTC_NSS,
            ls.FTC_CURP,
            ls.FTD_FEH_CRE, -- CONDICION
            ls.FTC_USU_CRE,
            ls.FTD_FEH_ACT,
            ls.FTC_USU_ACT,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_VIV DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            ls.FCN_ID_PROCESO,
            ls.FCN_ID_SUBPROCESO,
            ls.FCN_ID_ESTATUS,
            ls.FTB_IND_FOLIO_AGRUP,
            ls.FTC_NSS,
            ls.FTC_CURP,
            ls.FTD_FEH_CRE, -- CONDICION
            ls.FTC_USU_CRE,
            ls.FTD_FEH_ACT,
            ls.FTC_USU_ACT,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_COMP DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            ls.FCN_ID_PROCESO,
            ls.FCN_ID_SUBPROCESO,
            ls.FCN_ID_ESTATUS,
            ls.FTB_IND_FOLIO_AGRUP,
            ls.FTC_NSS,
            ls.FTC_CURP,
            ls.FTD_FEH_CRE, -- CONDICION
            ls.FTC_USU_CRE,
            ls.FTD_FEH_ACT,
            ls.FTC_USU_ACT,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_SAR DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            ls.FCN_ID_PROCESO,
            ls.FCN_ID_SUBPROCESO,
            ls.FCN_ID_ESTATUS,
            ls.FTB_IND_FOLIO_AGRUP,
            ls.FTC_NSS,
            ls.FTC_CURP,
            ls.FTD_FEH_CRE, -- CONDICION
            ls.FTC_USU_CRE,
            ls.FTD_FEH_ACT,
            ls.FTC_USU_ACT,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_AVOL DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            ls.FCN_ID_PROCESO,
            ls.FCN_ID_SUBPROCESO,
            ls.FCN_ID_ESTATUS,
            ls.FTB_IND_FOLIO_AGRUP,
            ls.FTC_NSS,
            ls.FTC_CURP,
            ls.FTD_FEH_CRE, -- CONDICION
            ls.FTC_USU_CRE,
            ls.FTD_FEH_ACT,
            ls.FTC_USU_ACT,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_BONO DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            ),
            TMSISGRAL AS (
            SELECT
            tmsmni.TMC_USO,
            tmsmni.TMN_ID_CONFIG,
            tmsmni.TMC_DESC_ITGY,
            tmsmni.TMN_CVE_ITGY,
            tmsmni.TMC_DESC_NCI,
            tmsmni.TMN_CVE_NCI
            FROM CIERREN.TMSISGRAL_MAP_NCI_ITGY tmsmni
            ),
            TTCRXGRAL AS (
            SELECT
            DISTINCT ttcp.FCN_ID_SUBPROCESO
            FROM TTCRXGRAL_PAGO ttcp
            ),
            PINTAR_TRAMITE AS (
            SELECT
            tms.TMC_USO,
            tms.TMN_ID_CONFIG,
            tms.TMC_DESC_ITGY,
            tms.TMN_CVE_ITGY,
            tms.TMC_DESC_NCI,
            tms.TMN_CVE_NCI
            FROM TMSISGRAL tms
            INNER JOIN TTCRXGRAL ttc
            ON tms.TMN_CVE_NCI = ttc.FCN_ID_SUBPROCESO
            where
            tms.TMC_DESC_ITGY in ('T97', 'TRU' ,'TED', 'TNP', 'TPP', 'T73', 'TPR', 'TGF', 'TED', 'TPI', 'TPG', 'TRJ', 'TJU', 'TIV', 'TIX', 'TEI', 'TPP', 'RJP', 'TAI', 'TNI', 'TRE', 'PPI', 'RCI', 'TJI')
            and tms.TMN_CVE_ITGY IS NOT NULL
            ),
            PAGO_TIPO_BANCO AS(
            SELECT
            ttcp.FTC_FOLIO,
            ttcp.FCN_ID_PROCESO,
            ttcp.FCN_ID_SUBPROCESO,
            ttcp.FTC_FOLIO_LIQUIDACION,
            ttcp.FTD_FEH_CRE,
            ttcp.FCN_TIPO_PAGO,
            ttcp.FCC_CVE_BANCO,
            ttcp.FTN_ISR,
            thccc.FCN_ID_CAT_CATALOGO,
            thccc.FCC_VALOR
            FROM BENEFICIOS.TTCRXGRAL_PAGO ttcp
            INNER JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO thccc
            ON ttcp.FCC_CVE_BANCO = thccc.FCN_ID_CAT_CATALOGO
            )
            , DATOS_PAGO AS (
            SELECT
            FTC_FOLIO,
            FCN_ID_PROCESO,
            FCN_ID_SUBPROCESO,
            FCN_TIPO_PAGO,
            FTC_FOLIO_LIQUIDACION,
            FTD_FEH_CRE,
            FCC_CVE_BANCO,
            FTN_ISR,
            FCN_ID_CAT_CATALOGO,
            FCC_TIPO_BANCO,
            FCC_MEDIO_PAGO
            FROM (
            SELECT
            ROW_NUMBER() over (PARTITION BY FTC_FOLIO ORDER BY FTD_FEH_CRE DESC)  rownumid,
            ptp.FTC_FOLIO,
            ptp.FCN_ID_PROCESO,
            ptp.FCN_ID_SUBPROCESO,
            ptp.FCN_TIPO_PAGO,
            ptp.FTC_FOLIO_LIQUIDACION,
            ptp.FTD_FEH_CRE,
            ptp.FCC_CVE_BANCO,
            ptp.FTN_ISR,
            ptp.FCN_ID_CAT_CATALOGO,
            ptp.FCC_VALOR  AS FCC_TIPO_BANCO,
            thccc.FCC_DESC AS FCC_MEDIO_PAGO
            FROM PAGO_TIPO_BANCO ptp
            INNER JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO thccc
            ON ptp.FCN_TIPO_PAGO = thccc.FCN_ID_CAT_CATALOGO
            )
            WHERE rownumid = 1
            ), LIQUIDACIONES AS (
            SELECT
            FTC_FOLIO,
            FTC_FOLIO_REL,
            FTN_NUM_CTA_INVDUAL,
            FCN_ID_PROCESO,
            FCN_ID_SUBPROCESO,
            FCN_ID_ESTATUS,
            FTB_IND_FOLIO_AGRUP,
            FTD_FEH_CRE,
            FTD_FEH_LIQUIDACION,
            FTN_TIPO_AHORRO,
            SUM(FTF_MONTO_PESOS) AS FTF_MONTO_PESOS
            FROM (
            SELECT
            L.FTC_FOLIO,
            L.FTC_FOLIO_REL,
            L.FTN_NUM_CTA_INVDUAL,
            L.FCN_ID_PROCESO,
            L.FCN_ID_SUBPROCESO,
            L.FCN_ID_ESTATUS,
            L.FTB_IND_FOLIO_AGRUP,
            L.FTD_FEH_CRE,
            L.FTD_FEH_LIQUIDACION,
            CASE L.FCN_ID_TIPO_SUBCTA
            WHEN 15 THEN 1
            WHEN 16 THEN 1
            WHEN 17 THEN 1
            WHEN 18 THEN 1
            ELSE 0
            END FTN_TIPO_AHORRO,
            L.FTF_MONTO_PESOS
            FROM LIQ L
            )
            GROUP BY
            FTC_FOLIO,
            FTC_FOLIO_REL,
            FTN_NUM_CTA_INVDUAL,
            FCN_ID_PROCESO,
            FCN_ID_SUBPROCESO,
            FCN_ID_ESTATUS,
            FTB_IND_FOLIO_AGRUP,
            FTD_FEH_CRE,
            FTD_FEH_LIQUIDACION,
            FTN_TIPO_AHORRO
            )
            , DIS_TRANS AS (
            SELECT
            L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
            L.FTC_FOLIO AS FTC_FOLIO,
            L.FCN_ID_PROCESO AS FCN_ID_PROCESO,
            L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO,
            L.FCN_ID_ESTATUS,
            L.FTF_MONTO_PESOS AS FTF_MONTO_LIQUIDADO,
            T.FTC_TIPO_TRAMITE,
            T.FTC_CVE_TIPO_SEG FTC_TPSEGURO,
            T.FTC_CVE_REGIMEN FTC_REGIMEN,
            T.FTC_CVE_TIPO_PEN FTC_TPPENSION,
            PT.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
            PT.TMN_CVE_ITGY AS FTN_TMN_CVE_ITGY,
            PT.TMC_DESC_NCI AS FTC_TMC_DESC_NCI,
            PT.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
            T.FTC_FEH_INI_PEN AS FTN_FEH_INI_PEN,
            T.FTC_FEH_RES_PEN AS FTN_FEH_RES_PEN,
            L.FTN_TIPO_AHORRO,
            L.FTD_FEH_CRE
            FROM LIQUIDACIONES L
            INNER JOIN BENEFICIOS.TTAFORETI_TRAMITE T
            ON  L.FTC_FOLIO = T.FTC_FOLIO
            INNER JOIN PINTAR_TRAMITE PT
            ON T.FTC_TIPO_TRAMITE = PT.TMN_CVE_NCI
            UNION ALL
            SELECT
            L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
            L.FTC_FOLIO AS FTC_FOLIO,
            L.FCN_ID_PROCESO AS FCN_ID_PROCESO,
            L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO,
            L.FCN_ID_ESTATUS,
            L.FTF_MONTO_PESOS AS FTF_MONTO_LIQUIDADO,
            TR.FTC_TIPO_TRAMITE,
            TR.FCC_TPSEGURO AS FTC_TPSEGURO,
            TR.FTC_REGIMEN AS FTC_REGIMEN,
            TR.FCC_TPPENSION AS FTC_TPPENSION,
            PT.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
            PT.TMN_CVE_ITGY AS FTN_TMN_CVE_ITGY,
            PT.TMC_DESC_NCI AS FTC_TMC_DESC_NCI,
            PT.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
            NULL AS "FTN_FEH_INI_PEN",
            NULL AS "FTN_FEH_RES_PEN",
            L.FTN_TIPO_AHORRO,
            L.FTD_FEH_CRE
            FROM LIQUIDACIONES L
            INNER JOIN TTAFORETI_TRANS_RETI TR
            ON L.FTC_FOLIO = TR.FTC_FOLIO
            INNER JOIN PINTAR_TRAMITE PT
            ON TR.FTC_TIPO_TRAMITE = PT.TMN_CVE_NCI
            UNION ALL
            SELECT
            L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
            L.FTC_FOLIO AS FTC_FOLIO,
            L.FCN_ID_PROCESO AS FCN_ID_PROCESO,
            L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO,
            L.FCN_ID_ESTATUS,
            L.FTF_MONTO_PESOS AS FTF_MONTO_LIQUIDADO,
            TR.FTC_TIPO_TRAMITE,
            TR.FCC_TPSEGURO AS FTC_TPSEGURO,
            TR.FTC_REGIMEN AS FTC_REGIMEN,
            TR.FCC_TPPENSION AS FTC_TPPENSION,
            PT.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
            PT.TMN_CVE_ITGY AS FTN_TMN_CVE_ITGY,
            PT.TMC_DESC_NCI AS FTC_TMC_DESC_NCI,
            PT.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
            NULL AS "FTN_FEH_INI_PEN",
            NULL AS "FTN_FEH_RES_PEN",
            L.FTN_TIPO_AHORRO,
            L.FTD_FEH_CRE
            FROM LIQUIDACIONES L
            INNER JOIN THAFORETI_HIST_TRANS_RETI TR
            ON L.FTC_FOLIO = TR.FTC_FOLIO
            INNER JOIN PINTAR_TRAMITE PT
            ON TR.FTC_TIPO_TRAMITE = PT.TMN_CVE_NCI
            )
            SELECT
            D.FCN_CUENTA,
            D.FTC_FOLIO,
            D.FCN_ID_PROCESO,
            D.FCN_ID_SUBPROCESO,
            D.FCN_ID_ESTATUS,
            (D.FTF_MONTO_LIQUIDADO - P.FTN_ISR) AS FTF_MONTO_LIQUIDADO,
            P.FCC_CVE_BANCO,
            P.FTN_ISR,
            P.FCC_TIPO_BANCO,
            P.FCC_MEDIO_PAGO,
            D.FTC_TIPO_TRAMITE,
            D.FTC_TPSEGURO,
            D.FTC_REGIMEN,
            D.FTC_TPPENSION,
            D.FTC_TMC_DESC_ITGY,
            D.FTN_TMN_CVE_ITGY,
            D.FTC_TMC_DESC_NCI,
            D.FTN_TMN_CVE_NCI,
            D.FTN_FEH_INI_PEN,
            D.FTN_FEH_RES_PEN,
            D.FTN_TIPO_AHORRO,
            CASE D.FTC_TMC_DESC_ITGY
            WHEN 'RJP' THEN '73'
            WHEN 'T73' THEN '73'
            WHEN 'TED' THEN '73'
            WHEN 'TGF' THEN '73'
            WHEN 'TJU' THEN '73'
            WHEN 'TPI' THEN '73'
            WHEN 'TPP' THEN '73'
            WHEN 'TPR' THEN '73'
            WHEN 'TRE' THEN '73'
            WHEN 'TRJ' THEN '73'
            WHEN 'PPI' THEN '97'
            WHEN 'RCI' THEN '97'
            WHEN 'T97' THEN '97'
            WHEN 'TAI' THEN '97'
            WHEN 'TEI' THEN '97'
            WHEN 'TIV' THEN '97'
            WHEN 'TIX' THEN '97'
            WHEN 'TJI' THEN '97'
            WHEN 'TNI' THEN '97'
            WHEN 'TNP' THEN '97'
            WHEN 'TNP' THEN '97'
            END ARCHIVO,
            CASE FTC_TMC_DESC_ITGY
            WHEN 'RJP' THEN 'IMSS'
            WHEN 'T73' THEN 'IMSS'
            WHEN 'TED' THEN 'IMSS'
            WHEN 'TGF' THEN 'IMSS'
            WHEN 'TJU' THEN 'IMSS'
            WHEN 'TPI' THEN 'IMSS'
            WHEN 'TPP' THEN 'IMSS'
            WHEN 'TPR' THEN 'IMSS'
            WHEN 'TRE' THEN 'IMSS'
            WHEN 'TRJ' THEN 'IMSS'
            WHEN 'PPI' THEN 'ISSSTE'
            WHEN 'RCI' THEN 'ISSSTE'
            WHEN 'T97' THEN 'ISSSTE'
            WHEN 'TAI' THEN 'ISSSTE'
            WHEN 'TEI' THEN 'ISSSTE'
            WHEN 'TIV' THEN 'ISSSTE'
            WHEN 'TIX' THEN 'ISSSTE'
            WHEN 'TJI' THEN 'ISSSTE'
            WHEN 'TNI' THEN 'ISSSTE'
            WHEN 'TNP' THEN 'ISSSTE'
            WHEN 'TNP' THEN 'ISSSTE'
            END FTC_LEY_PENSION,
            CASE D.FTC_TMC_DESC_ITGY
            WHEN 'TAI' THEN 'ASEGURADORA'
            WHEN 'TGF' THEN 'GOBIERNO FEDERAL'
            END FTC_FON_ENTIDAD,
            D.FTD_FEH_CRE
            FROM DIS_TRANS D
            INNER JOIN DATOS_PAGO P
            ON D.FTC_FOLIO = P.FTC_FOLIO
        """

        query_saldos ="""
            WITH LIQ_SOLICITUDES AS (
            SELECT
            tthls.FTC_FOLIO,
            tthls.FTC_FOLIO_REL,
            tthls.FTN_NUM_CTA_INVDUAL,
            tthls.FCN_ID_PROCESO,
            tthls.FCN_ID_SUBPROCESO,
            tthls.FCN_ID_ESTATUS,
            tthls.FTB_IND_FOLIO_AGRUP,
            tthls.FTC_NSS,
            tthls.FTC_CURP,
            tthls.FTD_FEH_CRE, -- CONDICION
            tthls.FTC_USU_CRE,
            tthls.FTD_FEH_ACT,
            tthls.FTC_USU_ACT
            FROM BENEFICIOS.THAFORETI_HIST_LIQ_SOLICITUDES tthls
            WHERE tthls.FTB_IND_FOLIO_AGRUP = '1'
            AND tthls.FCN_ID_ESTATUS = 6649
            AND tthls.FCN_ID_PROCESO IN (4045, 4046, 4047, 4048, 4049, 4050, 4051)
            AND tthls.FTD_FEH_CRE BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy') --:start AND :end
            union all
            SELECT ttls.FTC_FOLIO,
            ttls.FTC_FOLIO_REL,
            ttls.FTN_NUM_CTA_INVDUAL,
            ttls.FCN_ID_PROCESO,
            ttls.FCN_ID_SUBPROCESO,
            ttls.FCN_ID_ESTATUS,
            ttls.FTB_IND_FOLIO_AGRUP,
            ttls.FTC_NSS,
            ttls.FTC_CURP,
            ttls.FTD_FEH_CRE, -- CONDICION
            ttls.FTC_USU_CRE,
            ttls.FTD_FEH_ACT,
            ttls.FTC_USU_ACT
            FROM BENEFICIOS.TTAFORETI_LIQ_SOLICITUDES ttls
            WHERE ttls.FTB_IND_FOLIO_AGRUP = '1'
            AND ttls.FCN_ID_ESTATUS = 6649
            AND ttls.FCN_ID_PROCESO IN (4045, 4046, 4047, 4048, 4049, 4050, 4051)
            AND ttls.FTD_FEH_CRE BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy')
            )
            , LIQ AS (
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_RCV DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_GOB DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_VIV DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_COMP DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_SAR DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_AVOL DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            UNION ALL
            SELECT
            ls.FTC_FOLIO,
            ls.FTC_FOLIO_REL,
            ls.FTN_NUM_CTA_INVDUAL,
            DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
            ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
            DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_BONO DT
            INNER JOIN LIQ_SOLICITUDES ls
            ON ls.FTC_FOLIO = DT.FTC_FOLIO AND ls.FTC_FOLIO_REL = DT.FTC_FOLIO_REL
            )
            , SALDOS_INIMES AS (
            SELECT
            SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
            --SH.FCN_ID_TIPO_SUBCTA,
            CASE
            WHEN SH.FCN_ID_TIPO_SUBCTA IN (15, 16) THEN 1
            ELSE 0
            END AS TIPO_AHORRO,
            SH.FTD_FEH_LIQUIDACION,
            ROUND(SUM(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION), 2) AS FTF_SALDO_DIA
            FROM cierren.thafogral_saldo_historico_v2 SH
            INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
            INNER JOIN (
            SELECT SHMAX.FTN_NUM_CTA_INVDUAL,
            SHMAX.FCN_ID_SIEFORE,
            SHMAX.FCN_ID_TIPO_SUBCTA,
            MAX(TRUNC(SHMAX.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
            FROM cierren.thafogral_saldo_historico_v2 SHMAX
            WHERE SHMAX.FTD_FEH_LIQUIDACION <= (
            SELECT MIN(SHMIN.FTD_FEH_LIQUIDACION)
            FROM cierren.thafogral_saldo_historico_v2 SHMIN
            WHERE SHMIN.FTD_FEH_LIQUIDACION > to_date('01/03/2023', 'dd/MM/yyyy')
            )
            AND SHMAX.FTN_NUM_CTA_INVDUAL IN (SELECT FTN_NUM_CTA_INVDUAL FROM LIQ)
            AND SHMAX.FCN_ID_TIPO_SUBCTA NOT IN (17, 18)
            -- AND SHMAX.FCN_ID_SIEFORE
            GROUP BY SHMAX.FTN_NUM_CTA_INVDUAL, SHMAX.FCN_ID_SIEFORE, SHMAX.FCN_ID_TIPO_SUBCTA
            ) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
            AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
            AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
            INNER JOIN (
            SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_SIEFORE, FCN_ID_REGIMEN ORDER BY FCD_FEH_ACCION DESC) AS ROW_NUM,
            FCN_ID_SIEFORE, FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
            FROM cierren.TCAFOGRAL_VALOR_ACCION
            WHERE FCD_FEH_ACCION <= to_date('01/03/2023', 'dd/MM/yyyy')
            ) VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
            AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
            AND VA.ROW_NUM = 1
            GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE,  SH.FTD_FEH_LIQUIDACION ,SH.FCN_ID_TIPO_SUBCTA
            )
            ,SALDOS_lIQ AS (
            SELECT
            SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
            1 as TIPO_AHORRO,
            SH.FTD_FEH_LIQUIDACION,
            ROUND(SUM(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION), 2) AS FTF_SALDO_DIA
            FROM cierren.thafogral_saldo_historico_v2 SH
            INNER JOIN (
            SELECT
            SHMAX.FTN_NUM_CTA_INVDUAL,
            SHMAX.FCN_ID_SIEFORE,
            SHMAX.FCN_ID_TIPO_SUBCTA,
            MAX(TRUNC(SHMAX.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
            FROM cierren.thafogral_saldo_historico_v2 SHMAX
            GROUP BY SHMAX.FTN_NUM_CTA_INVDUAL, SHMAX.FCN_ID_SIEFORE, SHMAX.FCN_ID_TIPO_SUBCTA
            ) SHMAXIMO
            ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
            AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA
            AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
            AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
            INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R
            ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
            INNER JOIN LIQ L
            ON L.FTN_NUM_CTA_INVDUAL = SH.FTN_NUM_CTA_INVDUAL
            AND L.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
            INNER JOIN cierren.TCAFOGRAL_VALOR_ACCION VA
            ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
            AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
            AND VA.FCD_FEH_ACCION = SH.FTD_FEH_LIQUIDACION
            where sh.FCN_ID_TIPO_SUBCTA in (17,18)
            GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, SH.FTD_FEH_LIQUIDACION
            )
            select
            FCN_CUENTA,
            FTC_TIPO_AHORRO,
            SUM(FTF_SALDO_DIA) AS FTF_SALDO_DIA
            from
            (
            select
            FCN_CUENTA,
            TIPO_AHORRO as FTN_TIPO_AHORRO,
            FTF_SALDO_DIA
            from SALDOS_LIQ
            union all
            select
            FCN_CUENTA,
            TIPO_AHORRO as FTC_TIPO_AHORRO,
            FTF_SALDO_DIA
            from SALDOS_INIMES
            ) x
            GROUP BY
            FCN_CUENTA,
            FTN_TIPO_AHORRO,
            FTF_SALDO_DIA
        """

        read_table_insert_temp_view(
            configure_mit_spark,
            query_liquidaciones,
            "liquidaciones",
            params={"end": end_month}
        )
        read_table_insert_temp_view(
            configure_mit_spark,
            query_saldos,
            "saldos",
            params={"end": end_month}
        )
        spark.sql("""
        Select l.*, s.* from
        liquidaciones l
        inner join saldos s
        on l.FCN_CUENTA = s.FCN_CUENTA and l.FTN_TIPO_AHORRO = s.FTN_TIPO_AHORRO
        """).show()



