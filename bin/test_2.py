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

phase = 11
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool,) as (postgres):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()
    spark.conf.set("spark.sql.files.maxPartitionBytes", "256m")
    spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 20)

    with register_time(postgres_pool, phase, term_id, user, area):

        query_saldos_aldia = """
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
                    WHERE X.FTN_NUM_CTA_INVDUAL IN (3200603494,3200145959,2530009924)
                )

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
                """

        query_saldos_inicio = """
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
                            WHERE X.FTN_NUM_CTA_INVDUAL IN (3200603494,3200145959,2530009924)
                        )
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
                        
                        """

        query_saldos_chequera = """
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
                            
                            WHERE X.FTN_NUM_CTA_INVDUAL IN (3200603494,3200145959,2530009924)
                        )
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
                        """


        read_table_insert_temp_view(
            configure_mit_spark,
            query=query_saldos_aldia,
            view="SALDOS_DIA",
            params={"end": end_month, 'start': start_month, 'term': term_id}
        )

        spark.sql("""select * from SALDOS_DIA""").show()

        read_table_insert_temp_view(
            configure_mit_spark,
            query=query_saldos_inicio,
            view="SALDOS_INICIALES",
            params={"end": end_month, 'start': start_month, 'term': term_id}
        )
        spark.sql("""select * from SALDOS_INICIALES""").show()

        read_table_insert_temp_view(
            configure_mit_spark,
            query=query_saldos_chequera,
            view="SALDOS_CHEQUERA",
            params={"end": end_month, 'start': start_month, 'term': term_id}
        )
        spark.sql("""select * from SALDOS_CHEQUERA""").show()






