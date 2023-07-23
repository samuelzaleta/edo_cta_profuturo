from profuturo.common import truncate_table, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_mit_pool, get_mit_url
from profuturo.extraction import extract_terms, extract_dataset_polars
import sys


postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = int(sys.argv[1])

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term=term_id):
        truncate_table(postgres, 'TEST_RETIROS', term=term_id)
        extract_dataset_polars(get_mit_url(), postgres, """
        WITH LIQ_SOLICITUDES AS (
            SELECT
                FTC_FOLIO,
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
            FROM BENEFICIOS.THAFORETI_HIST_LIQ_SOLICITUDES ttls
            WHERE ttls.FTB_IND_FOLIO_AGRUP = '1'
                AND ttls.FCN_ID_ESTATUS = 6649
                AND FCN_ID_PROCESO IN (4045, 4046, 4047, 4048, 4049, 4050, 4051)
                AND TO_CHAR(ttls.FTD_FEH_CRE, 'YYYYMM') = '202304'
        ),
        MOVIMIENTOS AS (
            SELECT
                ttem.FTC_FOLIO,
                ttem.FCN_ID_TIPO_SUBCTA,
                ttem.FCN_ID_SIEFORE,
                ttem.FTF_MONTO_PESOS
            FROM CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS ttem
        ),
        LIQUIDACIONES AS(
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
                ls.FTC_USU_ACT ,
                ms.FCN_ID_TIPO_SUBCTA,
                ms.FCN_ID_SIEFORE,
                ms.FTF_MONTO_PESOS
            FROM LIQ_SOLICITUDES ls
            LEFT JOIN MOVIMIENTOS ms
            ON ls.FTC_FOLIO = ms.FTC_FOLIO
        ),
        DISPOSICIONES AS (
            SELECT
                ttat.FTC_FOLIO,
                ttat.FTN_FOLIO_TRAMITE,
                ttat.FTC_TIPO_TRAMITE,
                ttat.FTC_TIPO_PRESTACION,
                ttat.FTC_CVE_REGIMEN,
                ttat.FTC_CVE_TIPO_SEG,
                ttat.FTC_CVE_TIPO_PEN,
                ttat.FTD_FEH_CRE
            FROM BENEFICIOS.TTAFORETI_TRAMITE ttat
            WHERE TO_CHAR(ttat.FTD_FEH_CRE, 'YYYYMM') = '202304'
                AND ttat.FTC_TIPO_TRAMITE NOT IN (314, 324, 341, 9542)
        ),
        LIQ_DIS AS (
            SELECT
                liq.FTC_FOLIO,
                liq.FTC_FOLIO_REL,
                liq.FTN_NUM_CTA_INVDUAL,
                liq.FCN_ID_PROCESO,
                liq.FCN_ID_SUBPROCESO,
                liq.FCN_ID_ESTATUS,
                liq.FTB_IND_FOLIO_AGRUP,
                liq.FTC_NSS,
                liq.FTC_CURP,
                liq.FTD_FEH_CRE, -- CONDICION
                liq.FTC_USU_CRE,
                liq.FTD_FEH_ACT,
                liq.FTC_USU_ACT ,
                liq.FCN_ID_TIPO_SUBCTA,
                liq.FCN_ID_SIEFORE,
                liq.FTF_MONTO_PESOS,
                dis.FTN_FOLIO_TRAMITE,
                dis.FTC_TIPO_TRAMITE,
                dis.FTC_TIPO_PRESTACION,
                dis.FTC_CVE_REGIMEN,
                dis.FTC_CVE_TIPO_SEG,
                dis.FTC_CVE_TIPO_PEN
            FROM LIQUIDACIONES liq
            LEFT JOIN DISPOSICIONES dis
            ON liq.FTC_FOLIO = dis.FTC_FOLIO
        ),
        TRANSFERENCIAS AS (
             SELECT
                ttatr.FTC_FOLIO,
                ttatr.FTC_FOLIO_PROCESAR,
                ttatr.FTC_FOLIO_TRANSACCION,
                ttatr.FCC_TPSEGURO,
                ttatr.FCC_TPPENSION,
                ttatr.FTC_REGIMEN,
                ttatr.FTD_FEH_CRE
            FROM BENEFICIOS.TTAFORETI_TRANS_RETI ttatr
            WHERE TO_CHAR(ttatr.FTD_FEH_CRE, 'YYYYMM') = '202304'
         ),
        LIQ_DIS_TRA AS (
            SELECT
                lid.FTC_FOLIO,
                lid.FTC_FOLIO_REL,
                lid.FTN_NUM_CTA_INVDUAL,
                lid.FCN_ID_PROCESO,
                lid.FCN_ID_SUBPROCESO,
                lid.FCN_ID_ESTATUS,
                lid.FTB_IND_FOLIO_AGRUP,
                lid.FTC_NSS,
                lid.FTC_CURP,
                lid.FTD_FEH_CRE, -- CONDICION
                lid.FTC_USU_CRE,
                lid.FTD_FEH_ACT,
                lid.FTC_USU_ACT ,
                lid.FCN_ID_TIPO_SUBCTA,
                lid.FCN_ID_SIEFORE,
                lid.FTF_MONTO_PESOS,
                lid.FTN_FOLIO_TRAMITE,
                lid.FTC_TIPO_TRAMITE,
                lid.FTC_TIPO_PRESTACION,
                lid.FTC_CVE_REGIMEN,
                lid.FTC_CVE_TIPO_SEG,
                lid.FTC_CVE_TIPO_PEN,
                tra.FTC_FOLIO_PROCESAR,
                tra.FTC_FOLIO_TRANSACCION,
                tra.FCC_TPSEGURO,
                tra.FCC_TPPENSION,
                tra.FTC_REGIMEN
            FROM LIQ_DIS lid
            LEFT JOIN TRANSFERENCIAS tra
            ON lid.FTC_FOLIO = tra.FTC_FOLIO
        ),
        TIPO_SUB_CUENTA AS (
            SELECT
                DISTINCT
                tcrts.FCN_ID_REGIMEN,
                tcrts.FCN_ID_TIPO_SUBCTA,
                tcrts.FCN_ID_CAT_SUBCTA,
                thccc.FCN_ID_CAT_CATALOGO,
                thccc.FCC_VALOR
            FROM CIERREN.TCCRXGRAL_TIPO_SUBCTA tcrts
            INNER JOIN CIERREN.THCRXGRAL_CAT_CATALOGO thccc
            ON tcrts.FCN_ID_REGIMEN = thccc.FCN_ID_CAT_CATALOGO
        ),
        LIQ_DIS_TRA_TSU AS (
            SELECT
                ldt.FTC_FOLIO,
                ldt.FTC_FOLIO_REL,
                ldt.FTN_NUM_CTA_INVDUAL,
                ldt.FCN_ID_PROCESO,
                ldt.FCN_ID_SUBPROCESO,
                ldt.FCN_ID_ESTATUS,
                ldt.FTB_IND_FOLIO_AGRUP,
                ldt.FTC_NSS,
                ldt.FTC_CURP,
                ldt.FTD_FEH_CRE, -- CONDICION
                ldt.FTC_USU_CRE,
                ldt.FTD_FEH_ACT,
                ldt.FTC_USU_ACT ,
                ldt.FCN_ID_TIPO_SUBCTA,
                ldt.FCN_ID_SIEFORE,
                ldt.FTF_MONTO_PESOS,
                ldt.FTN_FOLIO_TRAMITE,
                ldt.FTC_TIPO_TRAMITE,
                ldt.FTC_TIPO_PRESTACION,
                ldt.FTC_CVE_REGIMEN,
                ldt.FTC_CVE_TIPO_SEG,
                ldt.FTC_CVE_TIPO_PEN,
                ldt.FTC_FOLIO_PROCESAR,
                ldt.FTC_FOLIO_TRANSACCION,
                ldt.FCC_TPSEGURO,
                ldt.FCC_TPPENSION,
                ldt.FTC_REGIMEN,
                tsc.FCN_ID_REGIMEN,
                tsc.FCN_ID_CAT_SUBCTA,
                tsc.FCN_ID_CAT_CATALOGO,
                tsc.FCC_VALOR
            FROM LIQ_DIS_TRA ldt
            LEFT JOIN TIPO_SUB_CUENTA tsc
            ON ldt.FCN_ID_TIPO_SUBCTA = tsc.FCN_ID_TIPO_SUBCTA
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
            WHERE tmsmni.TMN_CVE_ITGY IS NULL
                AND tmsmni.TMC_USO = 'TIPO_TRAMITE_ACT'
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
            ON tms.TMN_CVE_NCI =  ttc.FCN_ID_SUBPROCESO
        ),
        LIQ_DIS_TRA_TSU_PTR AS (
            SELECT
                ldtt.FTC_FOLIO,
                ldtt.FTC_FOLIO_REL,
                ldtt.FTN_NUM_CTA_INVDUAL,
                ldtt.FCN_ID_PROCESO,
                ldtt.FCN_ID_SUBPROCESO,
                ldtt.FCN_ID_ESTATUS,
                ldtt.FTB_IND_FOLIO_AGRUP,
                ldtt.FTC_NSS,
                ldtt.FTC_CURP,
                ldtt.FTD_FEH_CRE, -- CONDICION
                ldtt.FTC_USU_CRE,
                ldtt.FTD_FEH_ACT,
                ldtt.FTC_USU_ACT ,
                ldtt.FCN_ID_TIPO_SUBCTA,
                ldtt.FCN_ID_SIEFORE,
                ldtt.FTF_MONTO_PESOS,
                ldtt.FTN_FOLIO_TRAMITE,
                ldtt.FTC_TIPO_TRAMITE,
                ldtt.FTC_TIPO_PRESTACION,
                ldtt.FTC_CVE_REGIMEN,
                ldtt.FTC_CVE_TIPO_SEG,
                ldtt.FTC_CVE_TIPO_PEN,
                ldtt.FTC_FOLIO_PROCESAR,
                ldtt.FTC_FOLIO_TRANSACCION,
                ldtt.FCC_TPSEGURO,
                ldtt.FCC_TPPENSION,
                ldtt.FTC_REGIMEN,
                ldtt.FCN_ID_REGIMEN,
                ldtt.FCN_ID_CAT_SUBCTA,
                ldtt.FCN_ID_CAT_CATALOGO,
                ldtt.FCC_VALOR,
                pt.TMC_USO,
                pt.TMN_ID_CONFIG,
                pt.TMC_DESC_ITGY,
                pt.TMN_CVE_ITGY,
                pt.TMC_DESC_NCI,
                pt.TMN_CVE_NCI
            FROM LIQ_DIS_TRA_TSU ldtt
            LEFT JOIN PINTAR_TRAMITE pt
            ON ldtt.FTC_TIPO_TRAMITE = pt.TMN_CVE_NCI
        ),
        PAGO_TIPO_BANCO AS(
            SELECT
                ttcp.FTC_FOLIO,
                ttcp.FCN_ID_PROCESO,
                ttcp.FCN_ID_SUBPROCESO,
                ttcp.FCN_TIPO_PAGO,
                ttcp.FTC_FOLIO_LIQUIDACION,
                ttcp.FTD_FEH_CRE,
                ttcp.FCC_CVE_BANCO,
                ttcp.FTN_ISR,
                thccc.FCN_ID_CAT_CATALOGO,
                thccc.FCC_VALOR
            FROM BENEFICIOS.TTCRXGRAL_PAGO ttcp
            LEFT JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO thccc
            ON ttcp.FCC_CVE_BANCO = thccc.FCN_ID_CAT_CATALOGO
            WHERE TO_CHAR(ttcp.FTD_FEH_CRE, 'YYYYMM') = '202304'
        ),
        DATOS_PAGO AS (
            SELECT
                ptp.FTC_FOLIO,
                ptp.FCN_ID_PROCESO,
                ptp.FCN_ID_SUBPROCESO,
                ptp.FCN_TIPO_PAGO,
                ptp.FTC_FOLIO_LIQUIDACION,
                ptp.FTD_FEH_CRE,
                ptp.FCC_CVE_BANCO,
                ptp.FTN_ISR,
                ptp.FCN_ID_CAT_CATALOGO,
                ptp.FCC_VALOR AS FCC_TIPO_BANCO,
                thccc.FCC_VALOR AS FCC_MEDIO_PAGO
            FROM PAGO_TIPO_BANCO ptp
            LEFT JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO thccc
            ON ptp.FCN_TIPO_PAGO = thccc.FCN_ID_CAT_CATALOGO
        )
        SELECT
            ldttp.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
            ldttp.FTC_FOLIO AS FTC_FOLIO_LDTTP,
            ldttp.FTC_FOLIO_REL,
            ldttp.FCN_ID_PROCESO AS FCN_ID_PROCESO_LDTTP,
            ldttp.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO_LDTTP,
            ldttp.FCN_ID_ESTATUS,
            ldttp.FTB_IND_FOLIO_AGRUP,
            ldttp.FTC_NSS,
            ldttp.FTC_CURP,
            ldttp.FTD_FEH_CRE, -- CONDICION
            ldttp.FTC_USU_CRE,
            ldttp.FTD_FEH_ACT,
            ldttp.FTC_USU_ACT ,
            ldttp.FCN_ID_TIPO_SUBCTA,
            ldttp.FCN_ID_SIEFORE,
            ldttp.FTF_MONTO_PESOS,
            ldttp.FTN_FOLIO_TRAMITE,
            ldttp.FTC_TIPO_TRAMITE,
            ldttp.FTC_TIPO_PRESTACION,
            ldttp.FTC_CVE_REGIMEN,
            ldttp.FTC_CVE_TIPO_SEG,
            ldttp.FTC_CVE_TIPO_PEN,
            ldttp.FTC_FOLIO_PROCESAR,
            ldttp.FTC_FOLIO_TRANSACCION,
            ldttp.FCC_TPSEGURO,
            ldttp.FCC_TPPENSION,
            ldttp.FTC_REGIMEN,
            ldttp.FCN_ID_REGIMEN,
            ldttp.FCN_ID_CAT_SUBCTA,
            ldttp.FCN_ID_CAT_CATALOGO AS FCN_ID_CAT_CATALOGO_LDTTP,
            ldttp.FCC_VALOR,
            ldttp.TMC_USO,
            ldttp.TMN_ID_CONFIG,
            ldttp.TMC_DESC_ITGY,
            ldttp.TMN_CVE_ITGY,
            ldttp.TMC_DESC_NCI,
            ldttp.TMN_CVE_NCI,
            dpg.FTC_FOLIO AS FTC_FOLIO_DPG,
            dpg.FCN_ID_PROCESO AS FCN_ID_PROCESO_DPG,
            dpg.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO_DPG,
            dpg.FCN_TIPO_PAGO,
            dpg.FTC_FOLIO_LIQUIDACION,
            -- dpg.FTD_FEH_CRE,
            dpg.FCC_CVE_BANCO,
            dpg.FTN_ISR,
            dpg.FCN_ID_CAT_CATALOGO AS FCN_ID_CAT_CATALOGO_DPG,
            dpg.FCC_TIPO_BANCO,
            dpg.FCC_MEDIO_PAGO
        FROM LIQ_DIS_TRA_TSU_PTR ldttp
        LEFT JOIN DATOS_PAGO dpg
        ON ldttp.FTC_FOLIO = dpg.FTC_FOLIO_LIQUIDACION
        """, "TEST_RETIROS", term=term_id)
