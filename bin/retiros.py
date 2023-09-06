from profuturo.common import truncate_table, register_time, define_extraction, notify
from profuturo.database import get_postgres_pool, get_mit_pool, configure_postgres_spark, configure_mit_spark
from profuturo.extraction import extract_terms, _get_spark_session, read_table_insert_temp_view, extract_dataset_spark
from profuturo.reporters import HtmlReporter
from pyspark.sql.functions import col
from warnings import filterwarnings
import sys


filterwarnings(action='ignore', category=DeprecationWarning, message='`np.bool` is a deprecated alias')
html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
phase = int(sys.argv[1])
area = int(sys.argv[4])
print(int(sys.argv[1]),int(sys.argv[2]),int(sys.argv[3]),int(sys.argv[4]))

with define_extraction(phase, postgres_pool, mit_pool) as (postgres, mit):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase,area ,term_id):
        truncate_table(postgres, 'TEST_RETIROS', term=term_id)
        extract_dataset_spark(
        configure_mit_spark,
        configure_postgres_spark, """
            WITH LIQ_SOLICITUDES AS (
                SELECT
                FTC_FOLIO,
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
                AND tthls.FTD_FEH_CRE BETWEEN to_date('01/03/2023','dd/MM/yyyy') AND to_date('30/03/2023','dd/MM/yyyy') --:start AND :end
                union all
                select
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
                from BENEFICIOS.TTAFORETI_LIQ_SOLICITUDES ttls
                WHERE ttls.FTB_IND_FOLIO_AGRUP = '1'
                AND ttls.FCN_ID_ESTATUS = 6649
                AND ttls.FCN_ID_PROCESO IN (4045, 4046, 4047, 4048, 4049, 4050, 4051)
                AND ttls.FTD_FEH_CRE BETWEEN to_date('01/03/2023','dd/MM/yyyy') AND to_date('30/03/2023','dd/MM/yyyy') --:start AND :end
            ),
            MOVIMIENTOS AS (
                SELECT
                ttem.FTC_FOLIO,
                ttem.FCN_ID_TIPO_SUBCTA,
                ttem.FCN_ID_SIEFORE,
                ttem.FTF_MONTO_PESOS
                FROM CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS ttem
                --WHERE ttem.FCD_FEH_CRE BETWEEN :start AND :end
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
                INNER JOIN MOVIMIENTOS ms
                ON ls.FTC_FOLIO = ms.FTC_FOLIO
                where ms.FTF_MONTO_PESOS > 0
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
                WHERE ttat.FTD_FEH_CRE BETWEEN  to_date('01/03/2023','dd/MM/yyyy') AND to_date('30/03/2023','dd/MM/yyyy') --:start AND :end
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
                WHERE ttatr.FTD_FEH_CRE BETWEEN to_date('01/03/2023','dd/MM/yyyy') AND to_date('30/03/2023','dd/MM/yyyy') --:start AND :end
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
                WHERE ttcp.FTD_FEH_CRE BETWEEN to_date('01/03/2023','dd/MM/yyyy') AND to_date('30/03/2023','dd/MM/yyyy') --:start AND :end
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
            ), SALDOS AS (
                SELECT 
                   SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
                   --SH.FCN_ID_SIEFORE,
                   SH.FCN_ID_TIPO_SUBCTA,
                   --SH.FTD_FEH_LIQUIDACION,
                   --:type AS FTC_TIPO_SALDO,
                   --MAX(VA.FCD_FEH_ACCION) AS FCD_FEH_ACCION,
                   --SUM(SH.FTN_DIA_ACCIONES) AS FTF_DIA_ACCIONES,
                   SUM(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION) AS FTF_SALDO_DIA
                FROM cierren.thafogral_saldo_historico_v2 SH
                INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
                INNER JOIN (
                    SELECT SHMAX.FTN_NUM_CTA_INVDUAL,
                           SHMAX.FCN_ID_SIEFORE,
                           SHMAX.FCN_ID_TIPO_SUBCTA,
                           MAX(TRUNC(SHMAX.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
                    FROM cierren.thafogral_saldo_historico_v2 SHMAX
                    WHERE SHMAX.FTD_FEH_LIQUIDACION <= to_date('01/03/2023','dd/MM/yyyy')
                    GROUP BY SHMAX.FTN_NUM_CTA_INVDUAL, SHMAX.FCN_ID_SIEFORE, SHMAX.FCN_ID_TIPO_SUBCTA
                ) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
                          AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
                          AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
                INNER JOIN (
                    SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_SIEFORE, FCN_ID_REGIMEN ORDER BY FCD_FEH_ACCION DESC) AS ROW_NUM,
                           FCN_ID_SIEFORE, FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
                    FROM cierren.TCAFOGRAL_VALOR_ACCION
                    WHERE FCD_FEH_ACCION <= to_date('01/03/2023','dd/MM/yyyy')
                ) VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
                    AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
                    AND VA.ROW_NUM = 1
                GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, SH.FTD_FEH_LIQUIDACION
            )
            SELECT
                ldttp.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
                ldttp.FTC_FOLIO AS FTC_FOLIO_LDTTP,
                ldttp.FTC_FOLIO_REL,
                ldttp.FCN_ID_PROCESO AS FCN_ID_PROCESO_LDTTP,
                ldttp.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO_LDTTP,
                ldttp.FCN_ID_ESTATUS,
                ldttp.FTB_IND_FOLIO_AGRUP AS FTC_IND_FOLIO_AGRUP,
                ldttp.FTC_NSS,
                ldttp.FTC_CURP,
                ldttp.FTD_FEH_CRE, -- CONDICION
                ldttp.FTC_USU_CRE,
                ldttp.FTD_FEH_ACT,
                ldttp.FTC_USU_ACT ,
                ldttp.FCN_ID_TIPO_SUBCTA,
                ldttp.FCN_ID_SIEFORE,
                ldttp.FTF_MONTO_PESOS as FTF_MONTO_PESOS_LIQUIDADO,
                ldttp.FTN_FOLIO_TRAMITE,
                ldttp.FTC_TIPO_TRAMITE,
                ldttp.FTC_TIPO_PRESTACION,
                ldttp.FTC_CVE_REGIMEN,
                ldttp.FTC_CVE_TIPO_SEG,
                ldttp.FTC_CVE_TIPO_PEN,
                ldttp.FTC_FOLIO_PROCESAR,
                ldttp.FTC_FOLIO_TRANSACCION,
                ldttp.FCC_TPSEGURO AS FTC_TPSEGURO,
                ldttp.FCC_TPPENSION AS FTC_TPPENSION,
                ldttp.FTC_REGIMEN,
                ldttp.FCN_ID_REGIMEN,
                ldttp.FCN_ID_CAT_SUBCTA,
                ldttp.FCN_ID_CAT_CATALOGO AS FCN_ID_CAT_CATALOGO_LDTTP,
                ldttp.FCC_VALOR AS FTC_LEY_PENSION,
                ldttp.TMC_USO AS FTC_TMC_USO,
                ldttp.TMN_ID_CONFIG AS FTN_TMN_ID_CONFIG,
                ldttp.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
                ldttp.TMN_CVE_ITGY AS FTN_TMN_CVE_ITGY,
                ldttp.TMC_DESC_NCI AS FTC_TMC_DESC_NCI,
                ldttp.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
                dpg.FTC_FOLIO AS FTC_FOLIO_DPG,
                dpg.FCN_ID_PROCESO AS FCN_ID_PROCESO_DPG,
                dpg.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO_DPG,
                dpg.FCN_TIPO_PAGO as FTN_TIPO_PAGO,
                dpg.FTC_FOLIO_LIQUIDACION,
                -- dpg.FTD_FEH_CRE,
                dpg.FCC_CVE_BANCO as FTC_CVE_BANCO,
                dpg.FTN_ISR as FTF_ISR,
                dpg.FCN_ID_CAT_CATALOGO AS FCN_ID_CAT_CATALOGO_DPG,
                dpg.FCC_TIPO_BANCO FTC_TIPO_BANCO,
                dpg.FCC_MEDIO_PAGO as FTC_MEDIO_PAGO,
                s.FTF_SALDO_DIA AS FTF_SALDO_INICIAL,
                CASE 
                    WHEN ldttp.FCN_ID_TIPO_SUBCTA = 14 THEN 0
                    WHEN ldttp.FCN_ID_TIPO_SUBCTA = 15 THEN 2
                    WHEN ldttp.FCN_ID_TIPO_SUBCTA = 16 THEN 2
                    ELSE 1
                END AS "FTN_TIPO_AHORRO"
            FROM LIQ_DIS_TRA_TSU_PTR ldttp
            LEFT JOIN DATOS_PAGO dpg
            ON ldttp.FTC_FOLIO = dpg.FTC_FOLIO
            INNER JOIN SALDOS s 
            on ldttp.FTN_NUM_CTA_INVDUAL = s.FCN_CUENTA and ldttp.FCN_ID_TIPO_SUBCTA = s.FCN_ID_TIPO_SUBCTA
            """, '"HECHOS"."TEST_RETIROS"',
        term = term_id,
        params = {"start": start_month, "end": end_month})

        read_table_insert_temp_view(
            configure_postgres_spark,
            """
            SELECT ROW_NUMBER() over (ORDER BY "FCN_CUENTA") id,
            * 
            FROM "HECHOS"."TEST_RETIROS"
            """,
            "retiros")

        columnas = [
                "id",
                "FCN_CUENTA",
                "FCN_ID_TIPO_SUBCTA",
                "FTN_TIPO_AHORRO",
                "FCN_ID_SIEFORE",
                "FTF_SALDO_INICIAL",
                "FTF_ISR",
                "FTF_MONTO_PESOS_LIQUIDADO",
                "FTC_TIPO_TRAMITE",
                "FTC_TIPO_PRESTACION",
                "FTC_LEY_PENSION",
                "FTC_CVE_REGIMEN",
                "FTC_CVE_TIPO_SEG",
                "FTC_CVE_TIPO_PEN",
                "FTC_TPSEGURO",
                "FTC_TPPENSION",
                "FTC_REGIMEN",
                "FCN_ID_REGIMEN",
                "FTC_TMC_DESC_ITGY",
                "FCN_ID_PROCESO_DPG",
                "FCN_ID_SUBPROCESO_DPG",
                "FTN_TIPO_PAGO",
                "FTC_CVE_BANCO",
                "FTC_TIPO_BANCO",
                "FTC_MEDIO_PAGO"
        ]

        # Cifras de control
        df = spark.sql("""
        WITH dataset as (select 
            id,
            FCN_CUENTA,
            FCN_ID_TIPO_SUBCTA,
            FTN_TIPO_AHORRO,
            FCN_ID_SIEFORE,
            FTF_SALDO_INICIAL,
            FTF_MONTO_PESOS_LIQUIDADO,
            FTF_ISR,
            FTC_TIPO_TRAMITE,
            FTC_TIPO_PRESTACION,
            FTC_LEY_PENSION,
            FTC_CVE_REGIMEN,
            FTC_CVE_TIPO_SEG,
            FTC_CVE_TIPO_PEN,
            FTC_TPSEGURO,
            FTC_TPPENSION,
            FTC_REGIMEN,
            FCN_ID_REGIMEN,
            FTC_TMC_DESC_ITGY,
            FCN_ID_PROCESO_DPG,
            FCN_ID_SUBPROCESO_DPG,
            FTN_TIPO_PAGO,
            FTC_CVE_BANCO,
            FTC_TIPO_BANCO,
            FTC_MEDIO_PAGO
        from retiros)
        SELECT 
            id,
            FCN_CUENTA,
            FCN_ID_TIPO_SUBCTA,
            IF(FTN_TIPO_AHORRO == 1, 'VOL', 'VIV') AS FTN_TIPO_AHORRO ,
            FCN_ID_SIEFORE,
            SUM(FTF_SALDO_INICIAL) AS FTF_SALDO_INICIAL,
            SUM(FTF_MONTO_PESOS_LIQUIDADO) AS FTF_MONTO_PESOS_LIQUIDADO,
            SUM(FTF_ISR) AS FTF_ISR,
            FTC_TIPO_TRAMITE,
            FTC_TIPO_PRESTACION,
            FTC_LEY_PENSION,
            FTC_CVE_REGIMEN,
            FTC_CVE_TIPO_SEG,
            FTC_CVE_TIPO_PEN,
            FTC_TPSEGURO,
            FTC_TPPENSION,
            FTC_REGIMEN,
            FCN_ID_REGIMEN,
            FTC_TMC_DESC_ITGY,
            FCN_ID_PROCESO_DPG,
            FCN_ID_SUBPROCESO_DPG,
            FTN_TIPO_PAGO,
            FTC_CVE_BANCO,
            FTC_TIPO_BANCO,
            FTC_MEDIO_PAGO
        FROM DATASET
        GROUP BY 1,2,3,4,5, 9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
        """)

        df.show(10)

        pandas_df = df.select(*[col(c).cast("string") for c in df.columns]).toPandas()

        # Obtener el valor máximo de id
        max_id = spark.sql("SELECT MAX(id) FROM retiros").collect()[0][0]

        pandas_df['id'] = pandas_df['id'].astype(int)
        # Dividir el resultado en tablas HTML de 50 en 50
        batch_size = 45500
        for start in range(0, max_id, batch_size):
            end = start + batch_size
            batch_pandas_df = pandas_df[(pandas_df['id'] >= start) & (pandas_df['id'] < end)]
            batch_html_table = batch_pandas_df.to_html(index=False)

            # Enviar notificación con la tabla HTML de este lote
            notify(
                postgres,
                f"Cifras de control Retiros generadas - Parte {start}-{end - 1}",
                "Se han generado las cifras de control para retiros exitosamente",
                batch_html_table,
                term=term_id,
                control=True,
                area = area
            )

