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
    end_month_anterior = term["end_saldos_anterior"]
    valor_accion_anterior = term["valor_accion_anterior"]
    print(end_month_anterior, valor_accion_anterior)
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        truncate_table(postgres, 'TTHECHOS_RETIRO', term=term_id)

        query_retiros = """
        WITH PINTAR_TRAMITE AS (
        SELECT
        distinct
        tms.TMC_DESC_ITGY,tms.TMC_DESC_NCI, tms.TMN_CVE_NCI, ttc.FCN_ID_SUBPROCESO
        FROM TMSISGRAL_MAP_NCI_ITGY tms
        INNER JOIN TTCRXGRAL_PAGO ttc
        ON tms.TMN_CVE_NCI = ttc.FCN_ID_SUBPROCESO
        where
        tms.TMC_DESC_ITGY in
        ('T73', 'TNP' ,'TPP', 'T97', 'TPR', 'TED', 'RJP', 'TRE', 'TJU', 'TEX', 'TGF', 'TPG', 'TRJ', 'TRU', 
        'TIV', 'TIX', 'TEI', 'TPI', 'TNI', 'TJI', 'PPI', 'RCI', 'TAI')
        )
        , LIQ_SOLICITUDES AS (
        SELECT
        X.FTN_NUM_CTA_INVDUAL, X.FTC_FOLIO, X.FTC_FOLIO_REL, PT.TMC_DESC_ITGY,
        PT.TMC_DESC_NCI, PT.TMN_CVE_NCI, X.FCN_ID_PROCESO, X.FCN_ID_SUBPROCESO,
        X.FTD_FEH_CRE
        FROM(
        SELECT
        ttls.FTC_FOLIO, ttls.FTC_FOLIO_REL, ttls.FTN_NUM_CTA_INVDUAL,
        ttls.FCN_ID_PROCESO, ttls.FCN_ID_SUBPROCESO, ttls.FTD_FEH_CRE
        FROM BENEFICIOS.THAFORETI_HIST_LIQ_SOLICITUDES ttls
        WHERE ttls.FTB_IND_FOLIO_AGRUP = '1'
        AND ttls.FCN_ID_ESTATUS = 6649
        --AND tthls.FCN_ID_PROCESO IN (4045, 4046, 4047, 4048, 4049, 4050, 4051)
        AND ttls.FTD_FEH_CRE BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy') --:start AND :end
        union all
        SELECT
        ttls.FTC_FOLIO, ttls.FTC_FOLIO_REL, ttls.FTN_NUM_CTA_INVDUAL,
        ttls.FCN_ID_PROCESO, ttls.FCN_ID_SUBPROCESO, ttls.FTD_FEH_CRE 
        FROM BENEFICIOS.TTAFORETI_LIQ_SOLICITUDES ttls
        WHERE ttls.FTB_IND_FOLIO_AGRUP = '1'
        AND ttls.FCN_ID_ESTATUS = 6649
        AND ttls.FTD_FEH_CRE BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy')
        ) X
        INNER JOIN PINTAR_TRAMITE PT
        ON PT.FCN_ID_SUBPROCESO = X.FCN_ID_SUBPROCESO
        ), DIS_TRANS AS (
        SELECT
        ROW_NUMBER() over (partition by FCN_CUENTA,FTC_FOLIO order by FTD_FEH_CRE desc) rown,
        FCN_CUENTA, FTC_FOLIO, FTC_FOLIO_REL, FCN_ID_PROCESO,FCN_ID_SUBPROCESO,
        FTC_TPSEGURO,
        FTC_REGIMEN,
        FTC_TPPENSION,
        FTC_TMC_DESC_ITGY,
        FTC_TMC_DESC_NCI,
        FTN_TMN_CVE_NCI,
        FTN_FEH_INI_PEN,
        FTN_FEH_RES_PEN,
        FTD_FEH_CRE
        FROM (
        SELECT
        L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
        L.FTC_FOLIO AS FTC_FOLIO,
        L.FTC_FOLIO_REL AS FTC_FOLIO_REL,
        L.FCN_ID_PROCESO AS FCN_ID_PROCESO,
        L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO,
        T.FTC_TIPO_TRAMITE,
        T.FTC_CVE_TIPO_SEG FTC_TPSEGURO,
        T.FTC_CVE_REGIMEN FTC_REGIMEN,
        T.FTC_CVE_TIPO_PEN FTC_TPPENSION,
        L.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
        L.TMC_DESC_NCI AS FTC_TMC_DESC_NCI,
        L.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
        T.FTC_FEH_INI_PEN AS FTN_FEH_INI_PEN,
        T.FTC_FEH_RES_PEN AS FTN_FEH_RES_PEN,
        L.FTD_FEH_CRE
        FROM LIQ_SOLICITUDES L
        INNER JOIN BENEFICIOS.TTAFORETI_TRAMITE T
        ON  L.FTC_FOLIO = T.FTC_FOLIO
        UNION ALL
        SELECT
        L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
        L.FTC_FOLIO AS FTC_FOLIO,
        L.FTC_FOLIO_REL,
        L.FCN_ID_PROCESO AS FCN_ID_PROCESO,
        TR.FTN_ID_SUBPRO_TRAMITE AS FCN_ID_SUBPROCESO,
        TR.FTC_TIPO_TRAMITE,
        TR.FCC_TPSEGURO AS FTC_TPSEGURO,
        TR.FTC_REGIMEN AS FTC_REGIMEN,
        TR.FCC_TPPENSION AS FTC_TPPENSION,
        L.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
        L.TMC_DESC_NCI AS FTC_TMC_DESC_NCI,
        L.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
        NULL AS "FTN_FEH_INI_PEN",
        NULL AS "FTN_FEH_RES_PEN",
        L.FTD_FEH_CRE
        FROM LIQ_SOLICITUDES L
        INNER JOIN TTAFORETI_TRANS_RETI TR
        ON L.FTC_FOLIO = TR.FTC_FOLIO_SOLICITUD
        UNION ALL
        SELECT
        L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
        L.FTC_FOLIO AS FTC_FOLIO,
        L.FTC_FOLIO_REL AS FTC_FOLIO_REL,
        TR.FTN_ID_SUBPRO_TRAMITE AS FCN_ID_PROCESO,
        L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO,
        TR.FTC_TIPO_TRAMITE,
        TR.FCC_TPSEGURO AS FTC_TPSEGURO,
        TR.FTC_REGIMEN AS FTC_REGIMEN,
        TR.FCC_TPPENSION AS FTC_TPPENSION,
        L.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
        L.TMC_DESC_NCI AS FTC_TMC_DESC_NCI,
        L.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
        NULL AS "FTN_FEH_INI_PEN",
        NULL AS "FTN_FEH_RES_PEN",
        L.FTD_FEH_CRE
        FROM LIQ_SOLICITUDES L
        INNER JOIN THAFORETI_HIST_TRANS_RETI TR
        ON L.FTC_FOLIO = TR.FTC_FOLIO_SOLICITUD
        ) X
        ), DATOS_PAGO AS(
        SELECT
        FTC_FOLIO,
        CASE
        WHEN COUNT(DISTINCT FCC_VALOR) > 1 THEN 'MultiplesTiposDeBancos'
        ELSE MAX(FCC_VALOR )
        END AS FCC_TIPO_BANCO,
        CASE
        WHEN COUNT(DISTINCT FCC_DESC ) > 1 THEN 'MultiplesMedioPago'
        ELSE MAX(FCC_DESC)
        END AS FCC_MEDIO_PAGO,
        sum(FTN_ISR) as FTN_ISR
        FROM (
        SELECT
        ttcp.FTC_FOLIO,
        ttcp.FTN_ID_ASOCIADO,
        ttcp.FTN_ISR,
        thccc.FCC_VALOR,
        thcccc.FCC_DESC,
        ttcp.FTN_NUM_REEXP
        FROM BENEFICIOS.TTCRXGRAL_PAGO ttcp
        INNER JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO thccc
        ON ttcp.FCC_CVE_BANCO = thccc.FCN_ID_CAT_CATALOGO
        INNER JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO thcccc
        ON ttcp.FCN_TIPO_PAGO = thcccc.FCN_ID_CAT_CATALOGO
        where ttcp.FTD_FEH_CRE BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy')
        and (FTC_FOLIO, FTN_ID_ASOCIADO, FTN_NUM_REEXP) IN (
        SELECT FTC_FOLIO,FTN_ID_ASOCIADO, MAX(FTN_NUM_REEXP)
        FROM BENEFICIOS.TTCRXGRAL_PAGO ttcp
        GROUP BY FTC_FOLIO, FTN_ID_ASOCIADO)
        )
        GROUP BY
        FTC_FOLIO
        )
        SELECT
        X.FCN_CUENTA,
        X.FTC_FOLIO,
        X.FTC_FOLIO_REL,
        FCN_ID_PROCESO,
        FCN_ID_SUBPROCESO,
        FTC_TPSEGURO,
        FTC_REGIMEN,
        FTC_TPPENSION,
        coalesce(DP.FTN_ISR,0) AS FTN_ISR,
        DP.FCC_TIPO_BANCO,
        DP.FCC_MEDIO_PAGO,
        FTC_TMC_DESC_ITGY,
        FTC_TMC_DESC_NCI,
        FTN_TMN_CVE_NCI,
        coalesce(FTN_FEH_INI_PEN, '00010101') AS FTN_FEH_INI_PEN,
        coalesce(FTN_FEH_RES_PEN, '00010101') AS FTN_FEH_RES_PEN,
        CASE FTC_TMC_DESC_ITGY
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
        WHEN 'TEX' THEN '73'
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
        END FTN_ARCHIVO,
        CASE FTC_TMC_DESC_ITGY
        WHEN 'T73' THEN 'IMSS'
        WHEN 'TNP' THEN 'IMSS'
        WHEN 'TPP' THEN 'IMSS'
        WHEN 'T97' THEN 'IMSS'
        WHEN 'TPR' THEN 'IMSS'
        WHEN 'TED' THEN 'IMSS'
        WHEN 'RJP' THEN 'IMSS'
        WHEN 'TRE' THEN 'IMSS'
        WHEN 'TJU' THEN 'IMSS'
        WHEN 'TEX' THEN 'IMSS'
        WHEN 'TGF' THEN 'IMSS'
        WHEN 'TPG' THEN 'IMSS'
        WHEN 'TRU' THEN 'IMSS'
        WHEN 'TIV' THEN 'IMSS'
        WHEN 'TIX' THEN 'ISSSTE'
        WHEN 'TEI' THEN 'ISSSTE'
        WHEN 'TPI' THEN 'ISSSTE'
        WHEN 'TNI' THEN 'ISSSTE'
        WHEN 'TJI' THEN 'ISSSTE'
        WHEN 'PPI' THEN 'ISSSTE'
        WHEN 'RCI' THEN 'ISSSTE'
        WHEN 'TAI' THEN 'ISSSTE'
        END FTC_LEY_PENSION,
        CASE FTC_TMC_DESC_ITGY
        WHEN 'TAI' THEN 'ASEGURADORA'
        WHEN 'TGF' THEN 'GOBIERNO FEDERAL'
        WHEN 'TIV' THEN 'GOBIERNO FEDERAL'
        WHEN 'TPG' THEN 'GOBIERNO FEDERAL'
        WHEN 'TRJ' THEN 'GOBIERNO FEDERAL'
        END FTC_FON_ENTIDAD,
        FTD_FEH_CRE
        FROM (
        SELECT
        FCN_CUENTA,
        FTC_FOLIO,
        FTC_FOLIO_REL,
        FCN_ID_PROCESO,
        FCN_ID_SUBPROCESO,
        FTC_TPSEGURO,
        FTC_REGIMEN,
        FTC_TPPENSION,
        FTC_TMC_DESC_ITGY,
        FTC_TMC_DESC_NCI,
        FTN_TMN_CVE_NCI,
        FTN_FEH_INI_PEN,
        FTN_FEH_RES_PEN,
        FTD_FEH_CRE
        FROM DIS_TRANS d
        where rown = 1
        ) X
        LEFT JOIN DATOS_PAGO DP
        ON DP.FTC_FOLIO = X.FTC_FOLIO
        """
        query_liquidaciones = """
        SELECT
        FTC_FOLIO,
        FTC_FOLIO_REL,
        FTN_NUM_CTA_INVDUAL,
        FTN_TIPO_AHORRO,
        SUM(FTF_MONTO_PESOS) AS FTF_MONTO_PESOS
        FROM(
        SELECT
        DT.FTC_FOLIO,
        DT.FTC_FOLIO_REL,
        DT.FTN_NUM_CTA_INVDUAL,
        DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
        DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
        CASE
        WHEN FCN_ID_TIPO_SUBCTA IN (14,15,17,18) THEN 1
        ELSE 0
        END FTN_TIPO_AHORRO,
        ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS
        FROM CIERREN.TTAFOGRAL_MOV_RCV DT
        WHERE DT.FTD_FEH_LIQUIDACION BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy') --:start AND :end
        UNION ALL
        SELECT
        DT.FTC_FOLIO,
        DT.FTC_FOLIO_REL,
        DT.FTN_NUM_CTA_INVDUAL,
        DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
        DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
        CASE
        WHEN FCN_ID_TIPO_SUBCTA IN (14,15,17,18) THEN 1
        ELSE 0
        END FTN_TIPO_AHORRO,
        ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS
        FROM CIERREN.TTAFOGRAL_MOV_GOB DT
        WHERE DT.FTD_FEH_LIQUIDACION BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy') --:start AND :end
        UNION ALL
        SELECT
        DT.FTC_FOLIO,
        DT.FTC_FOLIO_REL,
        DT.FTN_NUM_CTA_INVDUAL,
        DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
        DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
        CASE
        WHEN FCN_ID_TIPO_SUBCTA IN (14,15,17,18) THEN 1
        ELSE 0
        END FTN_TIPO_AHORRO,
        ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS
        FROM CIERREN.TTAFOGRAL_MOV_VIV DT
        WHERE DT.FTD_FEH_LIQUIDACION BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy') --:start AND :end
        UNION ALL
        SELECT
        DT.FTC_FOLIO,
        DT.FTC_FOLIO_REL,
        DT.FTN_NUM_CTA_INVDUAL,
        DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
        DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
        CASE
        WHEN FCN_ID_TIPO_SUBCTA IN (14,15,17,18) THEN 1
        ELSE 0
        END FTN_TIPO_AHORRO,
        ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS
        FROM CIERREN.TTAFOGRAL_MOV_COMP DT
        WHERE DT.FTD_FEH_LIQUIDACION BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy') --:start AND :end
        UNION ALL
        SELECT
        DT.FTC_FOLIO,
        DT.FTC_FOLIO_REL,
        DT.FTN_NUM_CTA_INVDUAL,
        DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
        DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
        CASE
        WHEN FCN_ID_TIPO_SUBCTA IN (14,15,17,18) THEN 1
        ELSE 0
        END FTN_TIPO_AHORRO,
        ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS
        FROM CIERREN.TTAFOGRAL_MOV_SAR DT
        WHERE DT.FTD_FEH_LIQUIDACION BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy') --:start AND :end
        UNION ALL
        SELECT
        DT.FTC_FOLIO,
        DT.FTC_FOLIO_REL,
        DT.FTN_NUM_CTA_INVDUAL,
        DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
        DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
        CASE
        WHEN FCN_ID_TIPO_SUBCTA IN (14,15,17,18) THEN 1
        ELSE 0
        END FTN_TIPO_AHORRO,
        ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS
        FROM CIERREN.TTAFOGRAL_MOV_AVOL DT
        WHERE DT.FTD_FEH_LIQUIDACION BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy') --:start AND :end
        UNION ALL
        SELECT
        DT.FTC_FOLIO,
        DT.FTC_FOLIO_REL,
        DT.FTN_NUM_CTA_INVDUAL,
        DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
        DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
        CASE
        WHEN FCN_ID_TIPO_SUBCTA IN (14,15,17,18) THEN 1
        ELSE 0
        END FTN_TIPO_AHORRO,
        ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS
        FROM CIERREN.TTAFOGRAL_MOV_BONO DT
        WHERE DT.FTD_FEH_LIQUIDACION BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy') --:start AND :end
            ) X
        GROUP BY
        FTC_FOLIO,
        FTC_FOLIO_REL,
        FTN_NUM_CTA_INVDUAL,
        FTN_TIPO_AHORRO
        """

        query_saldos = """
        WITH PINTAR_TRAMITE AS (
        SELECT
        distinct
        tms.TMC_DESC_ITGY,
        tms.TMC_DESC_NCI,
        tms.TMN_CVE_NCI,
        ttc.FCN_ID_SUBPROCESO
        FROM TMSISGRAL_MAP_NCI_ITGY tms
        INNER JOIN TTCRXGRAL_PAGO ttc
        ON tms.TMN_CVE_NCI = ttc.FCN_ID_SUBPROCESO
        where
        tms.TMC_DESC_ITGY in
        ('T73', 'TNP' ,'TPP', 'T97', 'TPR', 'TED', 'RJP', 'TRE', 'TJU',
        'TEX', 'TGF', 'TPG', 'TRJ', 'TRU', 'TIV', 'TIX', 'TEI', 'TPI',
        'TNI', 'TJI', 'PPI', 'RCI', 'TAI')
        )
        , RETIROS AS (
        SELECT
        X.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
        X.FTC_FOLIO,
        X.FTC_FOLIO_REL,
        PT.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
        X.FCN_ID_PROCESO,
        X.FCN_ID_SUBPROCESO,
        X.FTD_FEH_CRE
        FROM(
        SELECT
        ttls.FTC_FOLIO,
        ttls.FTC_FOLIO_REL,
        ttls.FTN_NUM_CTA_INVDUAL,
        ttls.FCN_ID_PROCESO,
        ttls.FCN_ID_SUBPROCESO,
        ttls.FTD_FEH_CRE
        FROM BENEFICIOS.THAFORETI_HIST_LIQ_SOLICITUDES ttls
        WHERE ttls.FTB_IND_FOLIO_AGRUP = '1'
        AND ttls.FCN_ID_ESTATUS = 6649
        --AND tthls.FCN_ID_PROCESO IN (4045, 4046, 4047, 4048, 4049, 4050, 4051)
        AND ttls.FTD_FEH_CRE BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy') --:start AND :end
        union all
        SELECT
        ttls.FTC_FOLIO,
        ttls.FTC_FOLIO_REL,
        ttls.FTN_NUM_CTA_INVDUAL,
        ttls.FCN_ID_PROCESO,
        ttls.FCN_ID_SUBPROCESO,
        ttls.FTD_FEH_CRE -- CONDICION
        FROM BENEFICIOS.TTAFORETI_LIQ_SOLICITUDES ttls
        WHERE ttls.FTB_IND_FOLIO_AGRUP = '1'
        AND ttls.FCN_ID_ESTATUS = 6649
        AND ttls.FTD_FEH_CRE BETWEEN to_date('01/03/2023', 'dd/MM/yyyy') AND to_date('31/03/2023', 'dd/MM/yyyy')
        ) X
        INNER JOIN PINTAR_TRAMITE PT
        ON PT.FCN_ID_SUBPROCESO = X.FCN_ID_SUBPROCESO
        )
        , SALDOS_AL_LIQUIDACION AS (SELECT
        FCN_CUENTA,
        FTN_TIPO_AHORRO
,
        FTC_TMC_DESC_ITGY,
        SUM(FTF_SALDO_DIA) AS FTF_SALDO_DIA
        FROM (
        SELECT
        DISTINCT
        SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
        SH.FTD_FEH_LIQUIDACION,
        RET.FTD_FEH_CRE,
        SH.FCN_ID_SIEFORE,
        SH.FCN_ID_TIPO_SUBCTA,
        RET.FTC_TMC_DESC_ITGY,
        CASE
        WHEN SH.FCN_ID_TIPO_SUBCTA IN (17,18) THEN 1
        ELSE 0
        END FTN_TIPO_AHORRO,
        VA.FCD_FEH_ACCION AS FCD_FEH_ACCION,
        VA.FCN_VALOR_ACCION AS VALOR_ACCION,
        SH.FTN_DIA_ACCIONES AS FTF_DIA_ACCIONES,
        ROUND(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION,2) AS FTF_SALDO_DIA
        FROM cierren.thafogral_saldo_historico_v2 SH
        INNER JOIN RETIROS RET ON SH.FTN_NUM_CTA_INVDUAL = RET.FCN_CUENTA
        INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        INNER JOIN (
        SELECT
        DISTINCT
        SH.FTN_NUM_CTA_INVDUAL,
        SH.FCN_ID_TIPO_SUBCTA,
        SH.FCN_ID_SIEFORE,
        MAX(TRUNC(SH.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
        FROM CIERREN.THAFOGRAL_SALDO_HISTORICO_V2 SH
        INNER JOIN RETIROS RET ON SH.FTN_NUM_CTA_INVDUAL = RET.FCN_CUENTA
        WHERE SH.FTD_FEH_LIQUIDACION <= RET.FTD_FEH_CRE
        AND SH.FCN_ID_TIPO_SUBCTA NOT IN (15,16)
        GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_TIPO_SUBCTA, SH.FCN_ID_SIEFORE
        ) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
        AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
        AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
        INNER JOIN cierren.TCAFOGRAL_VALOR_ACCION VA ON
        VA.FCD_FEH_ACCION  = TO_DATE(TO_CHAR(RET.FTD_FEH_CRE, 'dd/MM/yyyy'),'dd/MM/yyyy')
        AND SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
        )
        X
        GROUP BY
        FCN_CUENTA,
        FTN_TIPO_AHORRO,
        FTC_TMC_DESC_ITGY
        )
        , SALDOS_INICIO_MES AS (
        SELECT
        SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
        1 as FTN_TIPO_AHORRO,
        RET.FTC_TMC_DESC_ITGY,
        SUM(ROUND(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION,2)) AS FTF_SALDO_DIA
        FROM cierren.thafogral_saldo_historico_v2 SH
        INNER JOIN RETIROS RET ON SH.FTN_NUM_CTA_INVDUAL = RET.FCN_CUENTA
        INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        INNER JOIN RETIROS RET ON RET.FCN_CUENTA = SH.FTN_NUM_CTA_INVDUAL
        INNER JOIN (
        SELECT
        SHMAX.FTN_NUM_CTA_INVDUAL,
        SHMAX.FCN_ID_SIEFORE,
        SHMAX.FCN_ID_TIPO_SUBCTA,
        MAX(TRUNC(SHMAX.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
        FROM cierren.thafogral_saldo_historico_v2 SHMAX
        INNER JOIN RETIROS RET ON RET.FCN_CUENTA = SHMAX.FTN_NUM_CTA_INVDUAL
        WHERE SHMAX.FTD_FEH_LIQUIDACION <= to_date('01/03/2023', 'dd/MM/yyyy')
        AND SHMAX.FCN_ID_TIPO_SUBCTA IN (15,16)
        GROUP BY
        SHMAX.FTN_NUM_CTA_INVDUAL,
        SHMAX.FCN_ID_SIEFORE,
        SHMAX.FCN_ID_TIPO_SUBCTA
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
        GROUP BY SH.FTN_NUM_CTA_INVDUAL, RET.FTC_TMC_DESC_ITGY
        ) , SALDOS_CHEQUERA AS (
        SELECT
        FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
        FTC_TMC_DESC_ITGY,
        FTN_TIPO_AHORRO,
        SUM(PESOS) AS FTF_SALDO_DIA
        FROM (
        SELECT
        FTN_NUM_CTA_INVDUAL,
        FCN_ID_TIPO_SUBCTA,
        r.FTC_TMC_DESC_ITGY,
        CASE
        WHEN FCN_ID_TIPO_SUBCTA IN (14,15,17,18) THEN 1
        ELSE 0
        END FTN_TIPO_AHORRO,
        FCN_ID_SIEFORE,
        SUM(FTN_DIA_PESOS) PESOS
        FROM TTAFOGRAL_BALANCE_MOVS_CHEQ q
        INNER JOIN RETIROS r
        on q.FTN_NUM_CTA_INVDUAL = r.FCN_CUENTA
        WHERE 1=1
        AND FTD_FEH_LIQUIDACION <  to_date('01/03/2023', 'dd/MM/yyyy')
        AND q.FCN_ID_SUBPROCESO IN (307,324,314,341,6827,7301,9542)
        AND R.FTC_TMC_DESC_ITGY IN ('TJU', 'TGF', 'TPG', 'TRJ', 'TRU', 'TIV')
        GROUP BY FTN_NUM_CTA_INVDUAL,FCN_ID_TIPO_SUBCTA,FCN_ID_SIEFORE, r.FTC_TMC_DESC_ITGY
        HAVING SUM(FTN_DIA_PESOS) > 0) X
        GROUP BY
        FTN_NUM_CTA_INVDUAL,
        FTC_TMC_DESC_ITGY,
        FTN_TIPO_AHORRO

        )
        SELECT
        FCN_CUENTA,
        FTN_TIPO_AHORRO,
        FTC_TMC_DESC_ITGY,
        SUM(FTF_SALDO_DIA) AS FTF_SALDO_DIA
        FROM (
        SELECT
        FCN_CUENTA,
        FTN_TIPO_AHORRO,
        FTC_TMC_DESC_ITGY,
        FTF_SALDO_DIA
        FROM SALDOS_AL_LIQUIDACION
        UNION ALL
        SELECT
        FCN_CUENTA,
        FTN_TIPO_AHORRO,
        FTC_TMC_DESC_ITGY,
        FTF_SALDO_DIA
        FROM SALDOS_INICIO_MES
        UNION ALL
        SELECT
        FCN_CUENTA,
        FTN_TIPO_AHORRO,
        FTC_TMC_DESC_ITGY,
        FTF_SALDO_DIA
        FROM SALDOS_CHEQUERA
        ) X
        GROUP BY
        FCN_CUENTA,
        FTN_TIPO_AHORRO,
        FTC_TMC_DESC_ITGY
        """

        read_table_insert_temp_view(
            configure_mit_spark,
            query_retiros,
            "RETIROS",
            params={"end": end_month}
        )

        read_table_insert_temp_view(
            configure_mit_spark,
            query_liquidaciones,
            "SALDOS_LIQUIDACIONES",
            params={"end": end_month}
        )

        read_table_insert_temp_view(
            configure_mit_spark,
            query_saldos,
            "SALDOS_INICIALES",
            params={"end": end_month}
        )

        df = spark.sql("""
        SELECT
        DISTINCT
        RET.FCN_CUENTA,
        RET.FTC_FOLIO,
        SL.FTN_TIPO_AHORRO,
        RET.FCN_ID_PROCESO,
        RET.FCN_ID_SUBPROCESO,
        RET.FTC_TPSEGURO,
        RET.FTC_REGIMEN,
        RET.FTC_TPPENSION,
        CASE SL.FTN_TIPO_AHORRO
        WHEN 0 THEN (SL.FTF_MONTO_PESOS - RET.FTN_ISR)
        WHEN 1 THEN SL.FTF_MONTO_PESOS
        END FTF_MONTO_LIQ,
        CASE SL.FTN_TIPO_AHORRO
        WHEN 0 THEN RET.FTN_ISR
        WHEN 1 THEN 0
        END FTN_ISR_LIQ,
        RET.FTN_ISR,
        RET.FCC_TIPO_BANCO,
        RET.FCC_MEDIO_PAGO,
        RET.FTC_TMC_DESC_ITGY,
        RET.FTC_TMC_DESC_NCI,
        RET.FTN_TMN_CVE_NCI,
        RET.FTN_FEH_INI_PEN,
        RET.FTN_FEH_RES_PEN,
        RET.FTN_ARCHIVO, 
        RET.FTC_LEY_PENSION,
        RET.FTC_FON_ENTIDAD,
        RET.FTD_FEH_CRE
        FROM RETIROS RET
        INNER JOIN SALDOS_LIQUIDACIONES SL
        ON RET.FTC_FOLIO = SL.FTC_FOLIO AND RET.FTC_FOLIO_REL = SL.FTC_FOLIO_REL
        WHERE RET.FTC_TMC_DESC_ITGY IN ('T73', 'TNP' ,'TPP', 'T97', 'TPR', 'TED', 'RJP', 'TRE', 
        'TEX', 'TIX', 'TEI', 'TPI',
        'TNI', 'TJI', 'PPI', 'RCI', 'TAI')
        UNION ALL
        SELECT
        DISTINCT 
        RET.FCN_CUENTA,
        RET.FTC_FOLIO,
        SL.FTN_TIPO_AHORRO,
        RET.FCN_ID_PROCESO,
        RET.FCN_ID_SUBPROCESO,
        RET.FTC_TPSEGURO,
        RET.FTC_REGIMEN,
        RET.FTC_TPPENSION,
        CASE SL.FTN_TIPO_AHORRO
        WHEN 0 THEN (SL.FTF_MONTO_PESOS - RET.FTN_ISR)
        WHEN 1 THEN SL.FTF_MONTO_PESOS
        END FTF_MONTO_LIQ,
        CASE SL.FTN_TIPO_AHORRO
        WHEN 0 THEN RET.FTN_ISR
        WHEN 1 THEN 0
        END FTN_ISR_LIQ,
        RET.FTN_ISR,
        RET.FCC_TIPO_BANCO,
        RET.FCC_MEDIO_PAGO,
        RET.FTC_TMC_DESC_ITGY,
        RET.FTC_TMC_DESC_NCI,
        RET.FTN_TMN_CVE_NCI,
        RET.FTN_FEH_INI_PEN,
        RET.FTN_FEH_RES_PEN,
        RET.FTN_ARCHIVO, 
        RET.FTC_LEY_PENSION,
        RET.FTC_FON_ENTIDAD,
        RET.FTD_FEH_CRE
        FROM RETIROS RET
        INNER JOIN SALDOS_LIQUIDACIONES SL
        ON RET.FTC_FOLIO = SL.FTC_FOLIO 
        WHERE RET.FTC_TMC_DESC_ITGY IN ('TJU', 'TGF', 'TPG', 'TRJ', 'TRU', 'TIV')
        """)

        df.show()

        df.createOrReplaceTempView("SALDOS_LIQUIDACIONES")

        df = spark.sql("""
                SELECT 
                SL.FCN_CUENTA,
                SL.FTC_FOLIO,
                SL.FTN_TIPO_AHORRO,
                SL.FCN_ID_PROCESO,
                SL.FCN_ID_SUBPROCESO,
                SL.FTC_TPSEGURO,
                SL.FTC_REGIMEN,
                SL.FTC_TPPENSION,
                SL.FTF_MONTO_LIQ,
                SL.FTN_ISR_LIQ,
                SL.FTN_ISR,
                SL.FCC_TIPO_BANCO,
                SL.FCC_MEDIO_PAGO,
                SL.FTC_TMC_DESC_ITGY,
                SL.FTC_TMC_DESC_NCI,
                SL.FTN_TMN_CVE_NCI,
                SL.FTN_FEH_INI_PEN,
                SL.FTN_FEH_RES_PEN,
                SL.FTN_ARCHIVO, 
                SL.FTC_LEY_PENSION,
                SL.FTC_FON_ENTIDAD,
                SL.FTD_FEH_CRE
                FROM SALDOS_LIQUIDACIONES SL
                FULL JOIN SALDOS_INICIALES SI
                ON SI.FCN_CUENTA = SL.FCN_CUENTA
                AND SI.FTN_TIPO_AHORRO = SL.FTN_TIPO_AHORRO
                """)
        df.show()

        df = df.withColumn("FCN_ID_PERIODO", lit(term_id))

        _write_spark_dataframe(df, configure_postgres_spark, '"HECHOS"."TTHECHOS_RETIRO_TEST"')

        # Convert PySpark DataFrame to pandas DataFrame
        pandas_df = df.toPandas()

        # Convert pandas DataFrame to HTML
        html_table = pandas_df.to_html()

        # Enviar notificación con la tabla HTML de este lote
        notify(
            postgres,
            f"retiros",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para retiros exitosamente para el periodo",
            details=html_table,
            visualiza = False
        )



