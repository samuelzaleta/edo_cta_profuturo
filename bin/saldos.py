from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool,get_postgres_oci_pool, configure_mit_spark, configure_postgres_spark, configure_postgres_oci_spark
from profuturo.extraction import extract_terms, _get_spark_session, read_table_insert_temp_view, _write_spark_dataframe, extract_dataset_spark
from profuturo.reporters import HtmlReporter
from pyspark.sql.functions import col, lit
from sqlalchemy import text
from datetime import datetime
import sys

spark = _get_spark_session()

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

cuentas = (
)
ftn_cuenta = ""

if len(cuentas) > 0:
    ftn_cuenta = f"AND FTN_NUM_CTA_INVDUAL IN {cuentas}"

with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    print('time_period',term_id)
    end_month = term["end_month"]
    print('end_month',end_month)
    start_next_mes = term["start_next_mes"]
    print('start_next_mes',start_next_mes)
    start_next_mes_valor_accion = term["start_next_mes_valor_accion"]
    print('start_next_mes_valor_accion', start_next_mes_valor_accion)


    with register_time(postgres_pool, phase, term_id, user, area):
        # Extracción
        truncate_table(postgres_oci, "THHECHOS_SALDO_HISTORICO", term=term_id)
        truncate_table(postgres_oci, "TTCALCUL_BONO", term=term_id)
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
                  WHERE SHMIN.FTD_FEH_LIQUIDACION > :start_next_mes
              )
               AND SHMAX.FCN_ID_SIEFORE NOT IN (81)
               {ftn_cuenta}
            GROUP BY SHMAX.FTN_NUM_CTA_INVDUAL, SHMAX.FCN_ID_SIEFORE, SHMAX.FCN_ID_TIPO_SUBCTA
        ) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
                  AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
                  AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
        INNER JOIN (
            SELECT 
            ROW_NUM, FCN_ID_SIEFORE,  FCN_ID_REGIMEN, 
            FCN_VALOR_ACCION, FCD_FEH_ACCION
            FROM (
            SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_SIEFORE, FCN_ID_REGIMEN ORDER BY FCD_FEH_ACCION DESC) AS ROW_NUM,
                   FCN_ID_SIEFORE, FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
            FROM cierren.TCAFOGRAL_VALOR_ACCION
            WHERE FCD_FEH_ACCION <= :end_month
            )
            WHERE ROW_NUM = 1
        ) VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
            AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
        GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, SH.FTD_FEH_LIQUIDACION
        """

        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_oci_spark,
            query,
            '"HECHOS"."THHECHOS_SALDO_HISTORICO"',
            term=term_id,
            params={"start_next_mes": start_next_mes, "end_month" :end_month, "type": "F"},
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
                  WHERE SHMIN.FTD_FEH_LIQUIDACION > :start_next_mes
              )
               AND SHMAX.FCN_ID_SIEFORE IN (81)
               {ftn_cuenta}
               --AND SHMAX.FCN_ID_SIEFORE IN (74000) --LINEA AGREGADA
            GROUP BY SHMAX.FTN_NUM_CTA_INVDUAL, SHMAX.FCN_ID_SIEFORE, SHMAX.FCN_ID_TIPO_SUBCTA
        ) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
                  AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
                  AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
        INNER JOIN (
            SELECT 
            ROW_NUM,
            FCN_ID_SIEFORE, 
            FCN_ID_REGIMEN, 
            FCN_VALOR_ACCION, 
            FCD_FEH_ACCION
            FROM (
            SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_SIEFORE, FCN_ID_REGIMEN ORDER BY FCD_FEH_ACCION DESC) AS ROW_NUM,
                   FCN_ID_SIEFORE, FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
            FROM cierren.TCAFOGRAL_VALOR_ACCION
            WHERE FCD_FEH_ACCION <= :valor_accion
            )
            WHERE ROW_NUM = 1
        ) VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
            AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
        GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, SH.FTD_FEH_LIQUIDACION
        """

        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_oci_spark,
            query,
            '"HECHOS"."THHECHOS_SALDO_HISTORICO"',
            term=term_id,
            params={"start_next_mes": start_next_mes,"valor_accion": start_next_mes_valor_accion, "type": "F"},
        )


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
            configure_postgres_oci_spark,
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


        # Elimina tablas temporales
        postgres_oci.execute(text("""
                        DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"
                        """))

        postgres_oci.execute(text("""
                        DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_SIEFORE"
                        """))

        # Extracción de tablas temporales
        query_temp = """
                        SELECT
                        "FTN_ID_TIPO_SUBCTA", "FCN_ID_REGIMEN", "FCN_ID_CAT_SUBCTA", "FCC_VALOR", "FTC_TIPO_CLIENTE"
                        FROM "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"
                        """
        extract_dataset_spark(
            configure_postgres_spark,
            configure_postgres_oci_spark,
            query_temp,
            '"MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"'
        )

        # Extracción de tablas temporales
        query_temp = """
                        SELECT
                        "FTN_ID_SIEFORE", "FTC_DESCRIPCION", "FTC_DESCRIPCION_CORTA", "FTC_SIEFORE"
                        FROM "MAESTROS"."TCDATMAE_SIEFORE"
                        """
        extract_dataset_spark(
            configure_postgres_spark,
            configure_postgres_oci_spark,
            query_temp,
            '"MAESTROS"."TCDATMAE_SIEFORE"'
        )

        query_cifras = """
        SELECT 
            :term AS PERIODO,
            TS."FCC_VALOR" AS TIPO_SUBCUENTA,
            S."FTC_DESCRIPCION_CORTA" AS SIEFORE,
            TRUNC(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_SALDO_DIA" ELSE 0 END)::numeric,2)AS SALDO_FINAL_PESOS,
            TRUNC(SUM(CASE WHEN SH."FTC_TIPO_SALDO" = 'F' THEN SH."FTF_DIA_ACCIONES" ELSE 0 END)::numeric,6) AS SALDO_FINAL_ACCIONES
        FROM 
            "HECHOS"."THHECHOS_SALDO_HISTORICO" SH
        INNER JOIN 
            "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" TS ON SH."FCN_ID_TIPO_SUBCTA" = TS."FTN_ID_TIPO_SUBCTA"
        INNER JOIN 
            "MAESTROS"."TCDATMAE_SIEFORE" S ON SH."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
            WHERE "FCN_ID_PERIODO" = :term
        GROUP BY TS."FCC_VALOR", S."FTC_DESCRIPCION_CORTA"
        """
        read_table_insert_temp_view(
            configure_postgres_oci_spark,
            query_cifras,
            "CIFRAS_SALDOS",
            params={"term": term_id}
        )

        df = spark.sql(""" select * from CIFRAS_SALDOS""")
        # Convert PySpark DataFrame to pandas DataFrame
        pandas_df = df.toPandas()

        # Convert pandas DataFrame to HTML
        html_table = pandas_df.to_html()

        notify(
            postgres,
            f"Saldos 1 de 2 ",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para saldos exitosamente para el periodo",
            details=html_table,
        )


        #Elimina tablas temporales
        postgres_oci.execute(text("""
        DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"
        """))

        postgres_oci.execute(text("""
        DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_SIEFORE"
        """))
