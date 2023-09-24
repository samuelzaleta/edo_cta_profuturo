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
        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_spark, """
            WITH LIQ_SOLICITUDES AS (
                SELECT tthls.FTC_FOLIO,
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
                         --:start AND :end
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
                INNER JOIN CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS  ms
                ON ls.FTC_FOLIO = ms.FTC_FOLIO
                --where ms.FTF_MONTO_PESOS > 0
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
                TMSISGRAL AS (
                SELECT
                tmsmni.TMC_USO,
                tmsmni.TMN_ID_CONFIG,
                tmsmni.TMC_DESC_ITGY,
                tmsmni.TMN_CVE_ITGY,
                tmsmni.TMC_DESC_NCI,
                tmsmni.TMN_CVE_NCI
                FROM CIERREN.TMSISGRAL_MAP_NCI_ITGY tmsmni
                --WHERE TMC_DESC_ITGY IN ('T97', 'TRU' ,'TED', 'TNP', 'TPP', 'T73', 'TPR', 'TGF', 'TED', 'TPI', 'TPG', 'TRJ', 'TJU', 'TIV', 'TIX', 'TEI', 'TPP', 'RJP', 'TAI', 'TNI', 'TRE', 'PPI', 'RCI', 'TJI')
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
                WHERE ttcp.FTD_FEH_CRE BETWEEN to_date('01/03/2023','dd/MM/yyyy') AND to_date('31/03/2023','dd/MM/yyyy') --:start AND :end
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
                ),
                SALDOS AS (
                SELECT
                SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
                SH.FCN_ID_TIPO_SUBCTA,
                SUM(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION) AS FTF_SALDO_DIA
                FROM cierren.thafogral_saldo_historico_v2 SH
                INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
                INNER JOIN (
                SELECT SHMAX.FTN_NUM_CTA_INVDUAL,
                       SHMAX.FCN_ID_SIEFORE,
                       SHMAX.FCN_ID_TIPO_SUBCTA,
                       MAX(TRUNC(SHMAX.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
                FROM cierren.thafogral_saldo_historico_v2 SHMAX
                INNER JOIN LIQUIDACIONES L ON L.FTN_NUM_CTA_INVDUAL = SHMAX.FTN_NUM_CTA_INVDUAL and L.FCN_ID_TIPO_SUBCTA = SHMAX.FCN_ID_TIPO_SUBCTA
                WHERE SHMAX.FTD_FEH_LIQUIDACION<= (SELECT MIN(SHMIN.FTD_FEH_LIQUIDACION) FROM cierren.thafogral_saldo_historico_v2 SHMIN
                                                      WHERE  SHMIN.FTD_FEH_LIQUIDACION > to_date('31/03/2022', 'dd/MM/yyyy')) ---- FECHA A CONSULTAR
                GROUP BY SHMAX.FTN_NUM_CTA_INVDUAL, SHMAX.FCN_ID_SIEFORE, SHMAX.FCN_ID_TIPO_SUBCTA
                ) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
                      AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
                      AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
                INNER JOIN (
                SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_SIEFORE, FCN_ID_REGIMEN ORDER BY FCD_FEH_ACCION DESC) AS ROW_NUM,
                       FCN_ID_SIEFORE, FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
                FROM cierren.TCAFOGRAL_VALOR_ACCION
                WHERE FCD_FEH_ACCION <= to_date('31/03/2023','dd/MM/yyyy')
                ) VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
                AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
                AND VA.ROW_NUM = 1
                GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, SH.FTD_FEH_LIQUIDACION
                )
                , DISPOSICIONES AS (
                SELECT
                L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
                L.FTC_FOLIO AS FTC_FOLIO_LDTTP,
                L.FCN_ID_PROCESO AS FCN_ID_PROCESO_LDTTP,
                L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO_LDTTP,
                L.FCN_ID_ESTATUS,
                L.FCN_ID_TIPO_SUBCTA,
                L.FCN_ID_SIEFORE,
                L.FTF_MONTO_PESOS AS FTF_MONTO_PESOS_LIQUIDADO,
                TT.FTC_TIPO_TRAMITE,
                TT.FTC_TIPO_PRESTACION,
                TT.FTC_CVE_REGIMEN,
                TT.FTC_CVE_TIPO_SEG,
                TT.FTC_CVE_TIPO_PEN,
                PT.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
                PT.TMN_CVE_ITGY AS FTN_TMN_CVE_ITGY,
                PT.TMC_DESC_NCI AS FTC_TMC_DESC_NCI,
                PT.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
                CASE
                WHEN L.FCN_ID_TIPO_SUBCTA = 14 THEN 0
                WHEN L.FCN_ID_TIPO_SUBCTA = 15 THEN 2
                WHEN L.FCN_ID_TIPO_SUBCTA = 16 THEN 2
                ELSE 1
                END AS "FTN_TIPO_AHORRO",
                L.FTD_FEH_CRE
                FROM LIQUIDACIONES L
                LEFT JOIN BENEFICIOS.TTAFORETI_TRAMITE TT
                ON  L.FTC_FOLIO = TT.FTC_FOLIO
                LEFT JOIN PINTAR_TRAMITE PT
                ON TT.FTC_TIPO_TRAMITE = PT.TMN_CVE_NCI
                ),TRANSFERENCIA AS (
                SELECT
                L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
                L.FTC_FOLIO AS FTC_FOLIO_LDTTP,
                L.FCN_ID_PROCESO AS FCN_ID_PROCESO_LDTTP,
                L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO_LDTTP,
                L.FCN_ID_ESTATUS,
                L.FCN_ID_TIPO_SUBCTA,
                L.FCN_ID_SIEFORE,
                L.FTF_MONTO_PESOS AS FTF_MONTO_PESOS_LIQUIDADO,
                TR.FTC_TIPO_TRAMITE,
                TR.FCC_TPSEGURO AS FTC_TPSEGURO,
                TR.FCC_TPPENSION AS FTC_TPPENSION,
                TR.FTC_REGIMEN AS FTC_REGIMEN,
                PT.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
                PT.TMN_CVE_ITGY AS FTN_TMN_CVE_ITGY,
                PT.TMC_DESC_NCI AS FTC_TMC_DESC_NCI,
                PT.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
                CASE
                WHEN L.FCN_ID_TIPO_SUBCTA = 14 THEN 0
                WHEN L.FCN_ID_TIPO_SUBCTA = 15 THEN 2
                WHEN L.FCN_ID_TIPO_SUBCTA = 16 THEN 2
                ELSE 1
                END AS "FTN_TIPO_AHORRO",
                L.FTD_FEH_CRE
                FROM LIQUIDACIONES L
                LEFT JOIN TTAFORETI_TRANS_RETI TR
                ON L.FTC_FOLIO = TR.FTC_FOLIO
                LEFT JOIN PINTAR_TRAMITE PT
                ON TR.FTC_TIPO_TRAMITE = PT.TMN_CVE_NCI
                )
                , TRANSFERENCIAS_HISTORICAS AS (
                SELECT
                L.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
                L.FTC_FOLIO AS FTC_FOLIO_LDTTP,
                L.FCN_ID_PROCESO AS FCN_ID_PROCESO_LDTTP,
                L.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO_LDTTP,
                L.FCN_ID_ESTATUS,
                L.FCN_ID_TIPO_SUBCTA,
                L.FCN_ID_SIEFORE,
                L.FTF_MONTO_PESOS AS FTF_MONTO_PESOS_LIQUIDADO,
                TR.FTC_TIPO_TRAMITE,
                TR.FCC_TPSEGURO AS FTC_TPSEGURO,
                TR.FCC_TPPENSION AS FTC_TPPENSION,
                TR.FTC_REGIMEN AS FTC_REGIMEN,
                PT.TMC_DESC_ITGY AS FTC_TMC_DESC_ITGY,
                PT.TMN_CVE_ITGY AS FTN_TMN_CVE_ITGY,
                PT.TMC_DESC_NCI AS FTC_TMC_DESC_NCI,
                PT.TMN_CVE_NCI AS FTN_TMN_CVE_NCI,
                CASE
                WHEN L.FCN_ID_TIPO_SUBCTA = 14 THEN 0
                WHEN L.FCN_ID_TIPO_SUBCTA = 15 THEN 2
                WHEN L.FCN_ID_TIPO_SUBCTA = 16 THEN 2
                ELSE 1
                END AS "FTN_TIPO_AHORRO",
                L.FTD_FEH_CRE
                FROM LIQUIDACIONES L
                LEFT JOIN THAFORETI_HIST_TRANS_RETI TR
                ON L.FTC_FOLIO = TR.FTC_FOLIO
                LEFT JOIN PINTAR_TRAMITE PT
                ON TR.FTC_TIPO_TRAMITE = PT.TMN_CVE_NCI
                ), LIQ AS (
                SELECT
                ROW_NUMBER() over (PARTITION BY FTC_FOLIO_LDTTP ORDER BY FCN_CUENTA) "ID",
                "FCN_CUENTA",
                "FTC_FOLIO_LDTTP",
                "FCN_ID_PROCESO_LDTTP",
                "FCN_ID_SUBPROCESO_LDTTP",
                "FCN_ID_ESTATUS",
                "FCN_ID_TIPO_SUBCTA",
                "FCN_ID_SIEFORE",
                "FTF_MONTO_PESOS_LIQUIDADO",
                "FTC_TIPO_TRAMITE",
                "FTC_TIPO_PRESTACION",
                "FTC_CVE_REGIMEN",
                "FTC_CVE_TIPO_SEG",
                "FTC_CVE_TIPO_PEN",
                "FTC_TPSEGURO",
                "FTC_TPPENSION",
                "FTC_REGIMEN",
                "FTC_TMC_DESC_ITGY",
                "FTN_TMN_CVE_ITGY",
                "FTC_TMC_DESC_NCI",
                "FTN_TMN_CVE_NCI",
                "FTN_TIPO_AHORRO",
                "FTD_FEH_CRE"
                FROM (
                SELECT
                    "FCN_CUENTA",
                    "FTC_FOLIO_LDTTP",
                    "FCN_ID_PROCESO_LDTTP",
                    "FCN_ID_SUBPROCESO_LDTTP",
                    "FCN_ID_ESTATUS",
                    "FCN_ID_TIPO_SUBCTA",
                    "FCN_ID_SIEFORE",
                    "FTF_MONTO_PESOS_LIQUIDADO",
                    "FTC_TIPO_TRAMITE",
                    "FTC_TIPO_PRESTACION",
                    "FTC_CVE_REGIMEN",
                    "FTC_CVE_TIPO_SEG",
                    "FTC_CVE_TIPO_PEN",
                    NULL AS "FTC_TPSEGURO",
                    NULL AS "FTC_TPPENSION",
                    NULL AS "FTC_REGIMEN",
                    "FTC_TMC_DESC_ITGY",
                    "FTN_TMN_CVE_ITGY",
                    "FTC_TMC_DESC_NCI",
                    "FTN_TMN_CVE_NCI",
                    "FTN_TIPO_AHORRO",
                    "FTD_FEH_CRE"
                FROM DISPOSICIONES
                UNION ALL
                SELECT
                    "FCN_CUENTA",
                    "FTC_FOLIO_LDTTP",
                    "FCN_ID_PROCESO_LDTTP",
                    "FCN_ID_SUBPROCESO_LDTTP",
                    "FCN_ID_ESTATUS",
                    "FCN_ID_TIPO_SUBCTA",
                    "FCN_ID_SIEFORE",
                    "FTF_MONTO_PESOS_LIQUIDADO",
                    "FTC_TIPO_TRAMITE",
                    NULL AS "FTC_TIPO_PRESTACION",
                    NULL AS "FTC_CVE_REGIMEN",
                    NULL AS "FTC_CVE_TIPO_SEG",
                    NULL AS "FTC_CVE_TIPO_PEN",
                    "FTC_TPSEGURO",
                    "FTC_TPPENSION",
                    "FTC_REGIMEN",
                    "FTC_TMC_DESC_ITGY",
                    "FTN_TMN_CVE_ITGY",
                    "FTC_TMC_DESC_NCI",
                    "FTN_TMN_CVE_NCI",
                    "FTN_TIPO_AHORRO",
                    "FTD_FEH_CRE"
                FROM TRANSFERENCIA
                UNION ALL
                SELECT
                    "FCN_CUENTA",
                    "FTC_FOLIO_LDTTP",
                    "FCN_ID_PROCESO_LDTTP",
                    "FCN_ID_SUBPROCESO_LDTTP",
                    "FCN_ID_ESTATUS",
                    "FCN_ID_TIPO_SUBCTA",
                    "FCN_ID_SIEFORE",
                    "FTF_MONTO_PESOS_LIQUIDADO",
                    "FTC_TIPO_TRAMITE",
                    NULL AS "FTC_TIPO_PRESTACION",
                    NULL AS "FTC_CVE_REGIMEN",
                    NULL AS "FTC_CVE_TIPO_SEG",
                    NULL AS "FTC_CVE_TIPO_PEN",
                    "FTC_TPSEGURO",
                    "FTC_TPPENSION",
                    "FTC_REGIMEN",
                    "FTC_TMC_DESC_ITGY",
                    "FTN_TMN_CVE_ITGY",
                    "FTC_TMC_DESC_NCI",
                    "FTN_TMN_CVE_NCI",
                    "FTN_TIPO_AHORRO",
                    "FTD_FEH_CRE"
                FROM TRANSFERENCIAS_HISTORICAS
                ) LDTTP
                )
                SELECT
                L."FCN_CUENTA",
                L."FTC_FOLIO_LDTTP",
                L."FCN_ID_PROCESO_LDTTP",
                L."FCN_ID_SUBPROCESO_LDTTP",
                L."FCN_ID_ESTATUS",
                L."FCN_ID_TIPO_SUBCTA",
                L."FCN_ID_SIEFORE",
                L."FTF_MONTO_PESOS_LIQUIDADO",
                L."FTC_TIPO_TRAMITE",
                L."FTC_TIPO_PRESTACION",
                L."FTC_CVE_REGIMEN",
                L."FTC_CVE_TIPO_SEG",
                L."FTC_CVE_TIPO_PEN",
                L."FTC_TPSEGURO",
                L."FTC_TPPENSION",
                L."FTC_REGIMEN",
                L."FTC_TMC_DESC_ITGY",
                L."FTN_TMN_CVE_ITGY",
                L."FTC_TMC_DESC_NCI",
                L."FTN_TMN_CVE_NCI",
                DPG.FCN_ID_SUBPROCESO AS FCN_ID_SUBPROCESO_DPG,
                DPG.FCN_TIPO_PAGO as FTN_TIPO_PAGO,
                DPG.FTC_FOLIO_LIQUIDACION,
                DPG.FTN_ISR as FTF_ISR,
                cast(DPG.FCC_CVE_BANCO as integer) as FTN_CLAVE_BANCO,
                DPG.FCC_TIPO_BANCO FTC_TIPO_BANCO,
                DPG.FCC_MEDIO_PAGO as FTC_MEDIO_PAGO,
                S.FTF_SALDO_DIA AS FTF_SALDO_INICIAL,
                L."FTN_TIPO_AHORRO",
                L."FTD_FEH_CRE"
                FROM LIQ L
                LEFT JOIN TIPO_SUB_CUENTA TSC
                ON L.FCN_ID_TIPO_SUBCTA = TSC.FCN_ID_TIPO_SUBCTA
                LEFT JOIN DATOS_PAGO DPG
                ON L.FTC_FOLIO_LDTTP = DPG.FTC_FOLIO
                INNER JOIN SALDOS S
                on L.FCN_CUENTA = S.FCN_CUENTA and L.FCN_ID_TIPO_SUBCTA = S.FCN_ID_TIPO_SUBCTA
                WHERE ID = 1
            """, '"HECHOS"."TTHECHOS_RETIRO"',
            term=term_id,
            params={"start": start_month, "end": end_month})

        read_table_insert_temp_view(
            configure_postgres_spark,
            """
            SELECT ROW_NUMBER() over (ORDER BY "FCN_CUENTA") id,
            * 
            FROM "HECHOS"."TTHECHOS_RETIRO"
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
            "FTN_CLAVE_BANCO",
            "FTC_TIPO_BANCO",
            "FTC_MEDIO_PAGO",
            "FTD_FEH_CRE"
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
            FTN_CLAVE_BANCO,
            FTC_TIPO_BANCO,
            FTC_MEDIO_PAGO,
            FTD_FEH_CRE
        from retiros)
        SELECT 
            id,
            FCN_CUENTA,
            FCN_ID_TIPO_SUBCTA,
            IF(FTN_TIPO_AHORRO == 1, 'RET', 'VIV') AS FTN_TIPO_AHORRO ,
            FCN_ID_SIEFORE,
            ROUND(SUM(FTF_SALDO_INICIAL),2) + ( ROUND(SUM(FTF_MONTO_PESOS_LIQUIDADO),2) + ROUND(SUM(FTF_ISR),2)) AS FTF_SALDO_INICIAL,
            ROUND(SUM(FTF_MONTO_PESOS_LIQUIDADO),2) AS FTF_MONTO_PESOS_LIQUIDADO,
            ROUND(SUM(FTF_ISR),2) AS FTF_ISR,
            ROUND(SUM(FTF_SALDO_INICIAL),2) AS SALDO_REMANENTE,
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
            FTN_CLAVE_BANCO,
            FTC_TIPO_BANCO,
            FTC_MEDIO_PAGO,
            FTD_FEH_CRE
        FROM DATASET
        GROUP BY 
        id,
         FCN_CUENTA,
            FCN_ID_TIPO_SUBCTA,
            FTN_TIPO_AHORRO,
            FCN_ID_SIEFORE,
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
            FTN_CLAVE_BANCO,
            FTC_TIPO_BANCO,
            FTC_MEDIO_PAGO,
            FTD_FEH_CRE
        """)

        df.show(10)

        pandas_df = df.select(*[col(c).cast("string") for c in df.columns]).toPandas()

        # Obtener el valor máximo de id
        max_id = spark.sql("SELECT MAX(id) FROM retiros").collect()[0][0]

        pandas_df['id'] = pandas_df['id'].astype(int)
        # Dividir el resultado en tablas HTML de 50 en 50
        batch_size = 10000
        for start in range(0, max_id, batch_size):
            end = start + batch_size
            batch_pandas_df = pandas_df[(pandas_df['id'] >= start) & (pandas_df['id'] < end)]
            batch_html_table = batch_pandas_df.to_html(index=False)

            # Enviar notificación con la tabla HTML de este lote
            notify(
                postgres,
                f"Cifras de control Retiros generadas - Parte {start}-{end - 1}",
                f"Se han generado las cifras de control para retiros exitosamente para el periodo {time_period}",
                batch_html_table,
                term=term_id,
                area=area
            )
