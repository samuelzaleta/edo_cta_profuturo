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
        distinct
        tms.TMC_DESC_ITGY,
        tms.TMC_DESC_NCI,
        tms.TMN_CVE_NCI
        FROM TMSISGRAL tms
        INNER JOIN TTCRXGRAL ttc
        ON tms.TMN_CVE_NCI = ttc.FCN_ID_SUBPROCESO
        where
        tms.TMC_DESC_ITGY in 
        ('T73', 'TNP' ,'TPP', 'T97', 'TPR', 'TED', 'RJP', 'TRE', 'TJU', 
            'TEX', 'TGF', 'TPG', 'TRJ', 'TRU', 'TIV', 'TIX', 'TEI', 'TPI', 
        'TNI', 'TJI', 'PPI', 'RCI', 'TAI')
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
        D.FTF_MONTO_LIQUIDADO AS FTF_SALDO_INI,
        CASE FTN_TIPO_AHORRO
        WHEN 0 THEN (D.FTF_MONTO_LIQUIDADO - P.FTN_ISR)
        WHEN 1 THEN D.FTF_MONTO_LIQUIDADO
        END FTF_MONTO_LIQ,
        CASE FTN_TIPO_AHORRO
        WHEN 0 THEN P.FTN_ISR
        WHEN 1 THEN 0
        END FTN_ISR_LIQ,
        P.FCC_TIPO_BANCO,
        P.FCC_MEDIO_PAGO,
        D.FTC_TIPO_TRAMITE,
        D.FTC_TPSEGURO,
        D.FTC_REGIMEN,
        D.FTC_TPPENSION,
        D.FTC_TMC_DESC_ITGY,
        D.FTC_TMC_DESC_NCI,
        D.FTN_TMN_CVE_NCI,
        coalesce(D.FTN_FEH_INI_PEN, '00010101') AS FTN_FEH_INI_PEN,
        coalesce(D.FTN_FEH_RES_PEN, '00010101') AS FTN_FEH_RES_PEN,
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
        END ARCHIVO,
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
        CASE D.FTC_TMC_DESC_ITGY
        WHEN 'TAI' THEN 'ASEGURADORA'
        WHEN 'TGF' THEN 'GOBIERNO FEDERAL'
        WHEN 'TIV' THEN 'GOBIERNO FEDERAL'
        WHEN 'TPG' THEN 'GOBIERNO FEDERAL'
        WHEN 'TRJ' THEN 'GOBIERNO FEDERAL'
        END FTC_FON_ENTIDAD,
        D.FTD_FEH_CRE
        FROM DIS_TRANS D
        INNER JOIN DATOS_PAGO P
        ON D.FTC_FOLIO = P.FTC_FOLIO
        """

        read_table_insert_temp_view(
            configure_mit_spark,
            query_liquidaciones,
            "retiros",
            params={"end": end_month}
        )

        df = spark.sql("""
        SELECT * FROM retiros
        """)
        df = df.withColumn("FCN_ID_PERIODO", lit(term_id))

        _write_spark_dataframe(df, configure_postgres_spark, '"HECHOS"."TTHECHOS_RETIRO"')

        df = spark.sql("""
                SELECT 
                FCN_CUENTA,
                FTF_SALDO_INI,
                FTF_MONTO_LIQ,
                FTN_ISR_LIQ,
                FCC_TIPO_BANCO,
                FCC_MEDIO_PAGO,
                FTC_TIPO_TRAMITE,
                FTC_TPSEGURO,
                FTC_REGIMEN,
                FTC_TPPENSION,
                FTC_TMC_DESC_ITGY,
                FTC_TMC_DESC_NCI,
                FTN_TMN_CVE_NCI,
                FTN_FEH_INI_PEN,
                FTN_FEH_RES_PEN,
                FTN_TIPO_AHORRO,
                ARCHIVO,
                FTC_LEY_PENSION,
                FTC_FON_ENTIDAD
                 FROM retiros
                """)

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



