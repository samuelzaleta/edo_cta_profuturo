from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool,get_postgres_oci_pool, configure_mit_spark, configure_postgres_spark, configure_postgres_oci_spark
from profuturo.extraction import extract_terms, _get_spark_session, read_table_insert_temp_view, _write_spark_dataframe, extract_dataset_spark
from profuturo.reporters import HtmlReporter
from pyspark.sql.functions import col, lit
from sqlalchemy import text
import sys
from datetime import datetime

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

cuentas = (10000851,10000861,10000868,10000872,1330029515,1350011161,1530002222,1700004823,3070006370,3200089837,
           3200231348,3200534369,3201895226,3201900769,3202077144,3202135111,3300118473,3300576485,3300797221,3300809724,
           3400764001,3500053269,3500058618,6120000991,6442107959,6442128265,6449009395,6449015130,10000884,10000885,
           10000887,10000888,10000889,10000890,10000891,10000892,10000893,10000894,10000895,10000896,10001041,10001042,
           10000898,10000899,10000900,10000901,10000902,10000903,10000904,10000905,10000906,10000907,10000908,10000909,
           10000910,10000911,10000912,10000913,10000914,10000915,10000916,10000917,10000918,10000919,10000920,10000921,
           10000922,10000923,10000924,10000925,10000927,10000928,10000929,10000930,10000931,10000932,10000933,10000934,
           10000935,10000936,10001263,10001264,10001265,10001266,10001267,10001268,10001269,10001270,10001271,10001272,
           10001274,10001275,10001277,10001278,10001279,10001280,10001281,10001282,10001283,10001284,10001285,10001286,
           10001288,10001289,10001290,10001292,10001293,10001294,10001296,10001297,10001298,10001299,10001300,10001301,
           10001305,10001306,10001307,10001308,10001309,10001311,10001312,10001314,10001315,10001316,10001317,10001318,
           10001319,10001320,10001321,10001322,10000896,10000898,10000790,10000791,10000792,10000793,10000794,10000795,
           10000797,10000798,10000799,10000800,10000801,10000802,10000803,10000804,10000805,10000806,10000807,10000808,
           10000809,10000810,10000811,10000812,10000813,10000814,10000815,10000816,10000817,10000818,10000819,10000820,
           10000821,10000822,10000823,10000824,10000825,10000826,10000827,10000828,10000830,10000832,10000833,10000834,
           10000835,10000836,10000837,10000838,10000839,10000840,10001098,10001099,10001100,10001101,10001102,10001103,
           10001104,10001105,10001106,10001107,10001108,10001109,10001110,10001111,10001112,10001113,10001114,10001115,
           10001116,10001117,10001118,10001119,10001120,10001121,10001122,10001123,10001124,10001125,10001126,10001127,
           10001128,10001129,10001130,10001131,10001132,10001133,10001134,10001135,10001136,10001137,10001138,10001139,
           10001140,10001141,10001142,10001143,10001145,10001146,10001147,10001148,10000991,10000992,10000993,10000994,
           10000995,10000996,10000997,10000998,10000999,10001000,10001001,10001002,10001003,10001004,10001005,10001006,
           10001007,10001008,10001009,10001010,10001011,10001012,10001013,10001014,10001015,10001016,10001017,10001018,
           10001019,10001020,10001021,10001023,10001024,10001025,10001026,10001027,10001029,10001030,10001031,10001032,
           10001033,10001034,10001035,10001036,10001037,10001038,10001039,10001040,3200089837,	3201423324,	3201693866,
           3202486462,	3300118473,	3300780661,	3300809724,	3300835243,	3400764001,	6120000991,	6130000050,	6442107959,
           6449015130,	6449061689,	6449083099,	8051533577,	8052970237,	1700004823,	3500058618,	3200231348,	3300576485,
           3500053269,	1530002222,	3200840759,	3201292580,	3202135111,	8052710429,	3202077144,	3200474366,	3200767640,
           3300797020,	3300797221,	3400958595,	3201900769,	3201895226,	3200534369,	1350011161,	3200996343,	1330029515,
           3200976872,	3201368726,	3070006370,	6449009395,	6442128265,	3201096947 )


with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    print(term_id)
    end_month = term["end_month"]
    print(end_month)
    start_next_mes = term["start_next_mes"]
    print(start_next_mes)

    spark = _get_spark_session(excuetor_memory = '16g',
    memory_overhead ='1g',
    memory_offhead ='1g',
    driver_memory ='2g',
    intances = 4,
    parallelims = 18000)

    with register_time(postgres_pool, phase, term_id, user, area):
        # Extracción
        query = f"""
        SELECT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               SH.FCN_ID_SIEFORE,
               SH.FCN_ID_TIPO_SUBCTA,
               SH.FTD_FEH_LIQUIDACION,
               :type AS FTC_TIPO_SALDO,
               MAX(VA.FCD_FEH_ACCION) AS FCD_FEH_ACCION,
               SUM(SH.FTN_DIA_ACCIONES) AS FTF_DIA_ACCIONES,
               SUM(ROUND(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION,2)) AS FTF_SALDO_DIA
        FROM cierren.thafogral_saldo_historico_v2 SH
        INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
        INNER JOIN (
            SELECT SHMAX.FTN_NUM_CTA_INVDUAL,
                   SHMAX.FCN_ID_SIEFORE,
                   SHMAX.FCN_ID_TIPO_SUBCTA,
                   MAX(TRUNC(SHMAX.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
            FROM cierren.thafogral_saldo_historico_v2 SHMAX
            WHERE SHMAX.FTD_FEH_LIQUIDACION < (
                  SELECT MIN(SHMIN.FTD_FEH_LIQUIDACION)
                  FROM cierren.thafogral_saldo_historico_v2 SHMIN
                  WHERE SHMIN.FTD_FEH_LIQUIDACION > :date
              )
              -- AND SHMAX.FTN_NUM_CTA_INVDUAL in {cuentas}
              -- AND SHMAX.FCN_ID_TIPO_SUBCTA = 14
               AND SHMAX.FCN_ID_SIEFORE NOT IN (81)
            GROUP BY SHMAX.FTN_NUM_CTA_INVDUAL, SHMAX.FCN_ID_SIEFORE, SHMAX.FCN_ID_TIPO_SUBCTA
        ) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
                  AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
                  AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
        INNER JOIN (
            SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_SIEFORE, FCN_ID_REGIMEN ORDER BY FCD_FEH_ACCION DESC) AS ROW_NUM,
                   FCN_ID_SIEFORE, FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
            FROM cierren.TCAFOGRAL_VALOR_ACCION
            WHERE FCD_FEH_ACCION <= :date
        ) VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
            AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
            AND VA.ROW_NUM = 1
        GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, SH.FTD_FEH_LIQUIDACION
        """

        truncate_table(postgres, "THHECHOS_SALDO_HISTORICO", term=term_id)

        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_spark,
            query,
            '"HECHOS"."THHECHOS_SALDO_HISTORICO"',
            term=term_id,
            params={"date": end_month, "type": "F"},
        )

        # Extracción
        query = f"""
                SELECT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
                       SH.FCN_ID_SIEFORE,
                       SH.FCN_ID_TIPO_SUBCTA,
                       SH.FTD_FEH_LIQUIDACION,
                       :type AS FTC_TIPO_SALDO,
                       MAX(VA.FCD_FEH_ACCION) AS FCD_FEH_ACCION,
                       SUM(SH.FTN_DIA_ACCIONES) AS FTF_DIA_ACCIONES,
                       SUM(ROUND(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION,2)) AS FTF_SALDO_DIA
                FROM cierren.thafogral_saldo_historico_v2 SH
                INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
                INNER JOIN (
                    SELECT SHMAX.FTN_NUM_CTA_INVDUAL,
                           SHMAX.FCN_ID_SIEFORE,
                           SHMAX.FCN_ID_TIPO_SUBCTA,
                           MAX(TRUNC(SHMAX.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
                    FROM cierren.thafogral_saldo_historico_v2 SHMAX
                    WHERE SHMAX.FTD_FEH_LIQUIDACION < (
                          SELECT MIN(SHMIN.FTD_FEH_LIQUIDACION)
                          FROM cierren.thafogral_saldo_historico_v2 SHMIN
                          WHERE SHMIN.FTD_FEH_LIQUIDACION > :date
                      )
                      -- AND SHMAX.FTN_NUM_CTA_INVDUAL in {cuentas}
                      -- AND SHMAX.FCN_ID_TIPO_SUBCTA = 14
                       AND SHMAX.FCN_ID_SIEFORE IN (81)
                    GROUP BY SHMAX.FTN_NUM_CTA_INVDUAL, SHMAX.FCN_ID_SIEFORE, SHMAX.FCN_ID_TIPO_SUBCTA
                ) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
                          AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
                          AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
                INNER JOIN (
                    SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_SIEFORE, FCN_ID_REGIMEN ORDER BY FCD_FEH_ACCION DESC) AS ROW_NUM,
                           FCN_ID_SIEFORE, FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
                    FROM cierren.TCAFOGRAL_VALOR_ACCION
                    WHERE FCD_FEH_ACCION <= :date
                ) VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
                    AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
                    AND VA.ROW_NUM = 1
                GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, SH.FTD_FEH_LIQUIDACION
                """

        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_spark,
            query,
            '"HECHOS"."THHECHOS_SALDO_HISTORICO"',
            term=term_id,
            params={"date": start_next_mes, "type": "F"},
        )
        truncate_table(postgres, 'TTCALCUL_BONO', term=term_id)

        query_dias_rend_bono = """
                SELECT RB.FTN_NUM_CTA_INVDUAL, RB.FTD_FEH_VALOR AS FTD_FECHA_REDENCION_BONO,
                CAST(EXTRACT(DAY FROM RB.FTD_FEH_VALOR - TRUNC(:end)) AS INTEGER) AS DIAS_PLAZO_NATURALES
                FROM TTAFOGRAL_OP_INVDUAL RB
                WHERE (RB.FTN_NUM_CTA_INVDUAL, RB.FTD_FEH_VALOR) IN (
                                        SELECT FTN_NUM_CTA_INVDUAL,
                                               MAX(FTD_FEH_VALOR)
                                        FROM TTAFOGRAL_OP_INVDUAL
                                        GROUP BY FTN_NUM_CTA_INVDUAL)
                """

        query_vector = """
                SELECT FTN_PLAZO, FTN_FACTOR FROM TTAFOGRAL_VECTOR
                """

        query_saldos = """
                SELECT "FCN_CUENTA", "FTF_DIA_ACCIONES", "FTF_SALDO_DIA"
                FROM "HECHOS"."THHECHOS_SALDO_HISTORICO"
                WHERE "FCN_ID_PERIODO" = :term
                and "FCN_ID_TIPO_SUBCTA"  = 14
                """

        read_table_insert_temp_view(
            configure_mit_spark,
            query_dias_rend_bono,
            "DIAS_REDENCION",
            params={"end": end_month}
        )

        read_table_insert_temp_view(
            configure_mit_spark,
            query_vector,
            "VECTOR",
            params={"end": end_month}
        )

        read_table_insert_temp_view(
            configure_postgres_spark,
            query_saldos,
            "SALDOS",
            params={"term": term_id}
        )

        df = spark.sql("""
                    SELECT 
                    DISTINCT 
                    S.FCN_CUENTA,
                    ROUND(S.FTF_DIA_ACCIONES,6) AS FTF_BON_NOM_ACC, 
                    ROUND(S.FTF_SALDO_DIA,2) AS FTF_BON_NOM_PES,
                    COALESCE(CASE 
                    WHEN S.FTF_DIA_ACCIONES > 0 THEN ROUND(S.FTF_DIA_ACCIONES * X.FTN_FACTOR,6) END, 0) FTF_BON_ACT_ACC,
                    COALESCE(CASE 
                    WHEN S.FTF_SALDO_DIA > 0 THEN ROUND(S.FTF_SALDO_DIA * X.FTN_FACTOR,2) END, 0) FTF_BON_ACT_PES,
                    FTD_FECHA_REDENCION_BONO AS FTD_FEC_RED_BONO,
                    FTN_FACTOR
                    FROM (SELECT 
                            DR.FTN_NUM_CTA_INVDUAL,DR.FTD_FECHA_REDENCION_BONO,
                            DR.DIAS_PLAZO_NATURALES, VT.FTN_FACTOR
                            FROM DIAS_REDENCION DR
                                INNER JOIN VECTOR VT
                                ON VT.FTN_PLAZO = DR.DIAS_PLAZO_NATURALES
                        ) X
                            INNER JOIN SALDOS S 
                            ON X.FTN_NUM_CTA_INVDUAL = S.FCN_CUENTA
                """)

        df = df.withColumn("FCN_ID_PERIODO", lit(term_id))

        _write_spark_dataframe(df, configure_postgres_oci_spark, '"HECHOS"."TTCALCUL_BONO"')

        # Cifras de control
        report1 = html_reporter.generate(
            postgres,
            """
            SELECT
                I."FTC_GENERACION" AS GENERACION,
                I."FTC_VIGENCIA" AS VIGENCIA,
                I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                I."FTC_ORIGEN" AS ORIGEN,
                TS."FCC_VALOR" AS TIPO_SUBCUENTA,
                S."FTC_DESCRIPCION_CORTA" AS SIEFORE,
                --ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2) AS SALDO_INICIAL_PESOS,
                TRUNC(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_SALDO_DIA" ELSE 0 END):: NUMERIC, 2)AS SALDO_FINAL_PESOS,
                --ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTN_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_INICIAL_ACCIONES,
                TRUNC(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_DIA_ACCIONES" ELSE 0 END):: NUMERIC, 6) AS SALDO_FINAL_ACCIONES
            FROM "HECHOS"."THHECHOS_SALDO_HISTORICO" SH
            INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON SH."FCN_CUENTA" = I."FCN_CUENTA"
            INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" TS ON SH."FCN_ID_TIPO_SUBCTA" = TS."FTN_ID_TIPO_SUBCTA"
            INNER JOIN "MAESTROS"."TCDATMAE_SIEFORE" S ON SH."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
            WHERE SH."FCN_ID_PERIODO" = :term and I."FCN_ID_PERIODO" = :term
            GROUP BY TS."FCC_VALOR", S."FTC_DESCRIPCION_CORTA",I."FTC_GENERACION" , I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN"
            """,
            ["Generación", "Vigencia", "tipo_cliente", "Origen", "Sub cuenta", "SIEFORE"],
            ["Saldo final en pesos", "Saldo final en acciones"],
            params={"term": term_id},
        )

        report2 = html_reporter.generate(
            postgres,
            """
            SELECT TS."FCC_VALOR" AS TIPO_SUBCUENTA,
                   S."FTC_DESCRIPCION_CORTA" AS SIEFORE,
                   --ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2) AS SALDO_INICIAL_PESOS,
                   TRUNC(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2)AS SALDO_FINAL_PESOS,
                   --ROUND(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'I' THEN SH."FTN_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_INICIAL_ACCIONES,
                   TRUNC(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_FINAL_ACCIONES
            FROM "HECHOS"."THHECHOS_SALDO_HISTORICO" SH
                INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" TS ON SH."FCN_ID_TIPO_SUBCTA" = TS."FTN_ID_TIPO_SUBCTA"
                INNER JOIN "MAESTROS"."TCDATMAE_SIEFORE" S ON SH."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
            WHERE "FCN_ID_PERIODO" = :term
            GROUP BY TS."FCC_VALOR", S."FTC_DESCRIPCION_CORTA"
            """,
            ["TIPO_SUBCUENTA", "SIEFORE"],
            ["Saldo final en pesos", "Saldo final en acciones"],
            params={"term": term_id},
        )

        notify(
            postgres,
            f"Saldos",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para saldos exitosamente para el periodo",
            details=report1,
        )
        notify(
            postgres,
            f"Saldos",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para saldos exitosamente para el periodo",
            details=report2,
        )
