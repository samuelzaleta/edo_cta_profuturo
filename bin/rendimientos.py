from profuturo.common import register_time, define_extraction, notify, truncate_table
from profuturo.database import configure_postgres_spark, get_postgres_pool,get_postgres_oci_pool, get_buc_pool, configure_postgres_oci_spark, configure_mit_spark
from profuturo.extraction import extract_dataset_spark,_get_spark_session, _write_spark_dataframe, read_table_insert_temp_view, _create_spark_dataframe
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
from sqlalchemy import text
import sys
from datetime import datetime
import os


spark = _get_spark_session(
        excuetor_memory='8g',
        memory_overhead='1g',
        memory_offhead='1g',
        driver_memory='2g',
        intances=4,
        parallelims=8000)

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()
bucket_name = os.getenv("BUCKET_DEFINITIVO")

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

cuentas = (
)
fcn_cuenta = ""
cuenta_itgy = ""
if len(cuentas) > 0:
    fcn_cuenta = f"""AND "FCN_CUENTA" IN {cuentas}"""
    cuenta_itgy = f"""AND "CSIE1_NUMCUE" IN {cuentas}"""




with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    end_month_anterior = term["end_saldos_anterior"]
    valor_accion_anterior = term["valor_accion_anterior"]
    print(end_month_anterior, valor_accion_anterior)


    with register_time(postgres_pool, phase, term_id, user, area):
        postgres_oci.execute(text(""" DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" """))

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


        all_user = f"""
        SELECT 
        DISTINCT C."FCN_CUENTA" AS "FCN_CUENTA", TS."FTN_ID_TIPO_SUBCTA" AS "FCN_ID_TIPO_SUBCTA"
        FROM "HECHOS"."TCHECHOS_CLIENTE" C,
        "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" TS
        WHERE 1=1 --and C."FCN_ID_PERIODO" = :term
        --AND C."FCN_CUENTA" = 1330029515
        {fcn_cuenta}
        """
        saldo_inicial_query = f"""
        SELECT 
        "FCN_CUENTA"
        ,"FCN_ID_TIPO_SUBCTA" 
        ,SUM("FTF_SALDO_DIA"::double precision) AS "FTF_SALDO_INICIAL"
        FROM (
        SELECT
            DISTINCT 
            tsh."FCN_CUENTA"
            ,tsh."FCN_ID_TIPO_SUBCTA" 
            ,tsh."FTF_SALDO_DIA"
        FROM
            "HECHOS"."THHECHOS_SALDO_HISTORICO" tsh
        WHERE
            1=1
            AND tsh."FCN_ID_PERIODO" = CAST(to_char(:date, 'YYYYMM') AS INT)   
            --AND tsh."FCN_CUENTA" = 1330029515
            {fcn_cuenta}
        ) x
        GROUP BY
            1=1
            , "FCN_CUENTA"
            , "FCN_ID_TIPO_SUBCTA"
        """

        saldo_final_query = f"""
        SELECT 
        DISTINCT 
        "FCN_CUENTA"
        ,"FCN_ID_TIPO_SUBCTA" 
        ,SUM(ROUND("FTF_SALDO_DIA", 2)) AS "FTF_SALDO_FINAL"
        FROM (
        SELECT
            DISTINCT 
            tsh."FCN_CUENTA"
            ,tsh."FCN_ID_TIPO_SUBCTA" 
            ,tsh."FTF_SALDO_DIA"
        FROM
            "HECHOS"."THHECHOS_SALDO_HISTORICO" tsh
        WHERE
            1=1
            AND tsh."FTC_TIPO_SALDO" = 'F'
            AND tsh."FCN_ID_PERIODO" = :term_id
            --AND tsh."FCN_CUENTA" = 1330029515 
            {fcn_cuenta}
        ) x
        GROUP BY
            1=1
            , "FCN_CUENTA"
            , "FCN_ID_TIPO_SUBCTA"
        """

        cargo_query = f"""
        SELECT DISTINCT "FCN_CUENTA", "FCN_ID_TIPO_SUBCTA", SUM(round("FTF_MONTO_PESOS",2)) AS "FTF_CARGO"
            FROM (
                SELECT DISTINCT "FCN_CUENTA",
                       "FCN_ID_TIPO_SUBCTA",
                       SUM("FTF_MONTO_PESOS") AS "FTF_MONTO_PESOS"
                FROM "HECHOS"."TTHECHOS_MOVIMIENTO"
                WHERE "FCN_ID_TIPO_MOVIMIENTO" = '180'
                  AND "FCN_ID_PERIODO" = :term_id
                  --AND "FCN_CUENTA" = 1330029515
                  {fcn_cuenta}
                  GROUP BY
                  "FCN_CUENTA",
                  "FCN_ID_TIPO_SUBCTA"
                UNION ALL
                SELECT DISTINCT "CSIE1_NUMCUE" AS "FCN_CUENTA",
                       "SUBCUENTA" AS "FCN_ID_TIPO_SUBCTA",
                        SUM("MONTO") AS "FTF_MONTO_PESOS"
                FROM "HECHOS"."TTHECHOS_MOVIMIENTOS_INTEGRITY"
                WHERE CAST("CSIE1_CODMOV" AS INT) > 500
                  AND "FCN_ID_PERIODO" = :term_id
                  --AND "CSIE1_NUMCUE" = 1330029515
                  {cuenta_itgy}
                  GROUP BY
                  "CSIE1_NUMCUE",
                  "SUBCUENTA"
            ) AS mov
            GROUP BY "FCN_CUENTA", "FCN_ID_TIPO_SUBCTA"
        """

        abono_query = f"""
        SELECT DISTINCT "FCN_CUENTA", "FCN_ID_TIPO_SUBCTA", SUM(round("FTF_MONTO_PESOS",2)) AS "FTF_ABONO"
        FROM (
            SELECT DISTINCT "FCN_CUENTA",
                   "FCN_ID_TIPO_SUBCTA",
                   SUM("FTF_MONTO_PESOS") AS "FTF_MONTO_PESOS"
            FROM "HECHOS"."TTHECHOS_MOVIMIENTO"
            WHERE "FCN_ID_TIPO_MOVIMIENTO" = '181'
              AND "FCN_ID_PERIODO" = :term_id
              --AND "FCN_CUENTA" = 1330029515
              {fcn_cuenta}
              GROUP BY
              "FCN_CUENTA",
              "FCN_ID_TIPO_SUBCTA"
            UNION ALL
            SELECT DISTINCT "CSIE1_NUMCUE" AS "FCN_CUENTA",
                   "SUBCUENTA" AS "FCN_ID_TIPO_SUBCTA",
                    SUM("MONTO") AS "FTF_MONTO_PESOS"
            FROM "HECHOS"."TTHECHOS_MOVIMIENTOS_INTEGRITY"
            WHERE CAST("CSIE1_CODMOV" AS INT) <= 500
              AND "FCN_ID_PERIODO" = :term_id
              --AND "CSIE1_NUMCUE" = 1330029515
              {cuenta_itgy}
              GROUP BY
              "CSIE1_NUMCUE",
              "SUBCUENTA"
        
        ) AS mov
        GROUP BY "FCN_CUENTA", "FCN_ID_TIPO_SUBCTA"
        """

        comision_query = f"""
        SELECT
        DISTINCT 
        C."FCN_CUENTA",
        C."FTN_TIPO_SUBCTA" as "FCN_ID_TIPO_SUBCTA",
        SUM(C."FTF_MONTO_PESOS"::double precision) AS "FTF_COMISION"
        FROM "HECHOS"."TTHECHOS_COMISION" C
        WHERE "FCN_ID_PERIODO" = :term_id
        --AND "FCN_CUENTA" = 1330029515
        {fcn_cuenta}
        GROUP BY C."FCN_CUENTA", C."FTN_TIPO_SUBCTA"
        """

        truncate_table(postgres_oci, "TTCALCUL_RENDIMIENTO", term=term_id)

        read_table_insert_temp_view(
            configure_postgres_oci_spark,
            query=all_user,
            view="clientes",
            params={"date": end_month_anterior,"term": term_id ,"type": "F", "accion": valor_accion_anterior}
        )

        read_table_insert_temp_view(
            configure_postgres_oci_spark,
            query=saldo_inicial_query,
            view="saldoinicial",
            params={"date": valor_accion_anterior, "type": "F", "accion": valor_accion_anterior}
        )

        read_table_insert_temp_view(
            configure_postgres_oci_spark,
            query=saldo_final_query,
            view="saldofinal",
            params={
                    "end_month": end_month,
                    "term_id": term_id}
        )

        read_table_insert_temp_view(
            configure_postgres_oci_spark,
            query=cargo_query,
            view="cargo",
            params={"term_id": term_id}
        )

        read_table_insert_temp_view(
            configure_postgres_oci_spark,
            abono_query,
            "abono",
            params={"term_id": term_id}
        )

        read_table_insert_temp_view(
            configure_postgres_oci_spark,
            query=comision_query,
            view="comision",
            params={"term_id": term_id}
        )

        df = spark.sql(f"""
        WITH saldos as (
        SELECT 
            DISTINCT 
            c.FCN_CUENTA AS FCN_CUENTA,
            c.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
            COALESCE(s.FTF_SALDO_INICIAL, 0) AS FTF_SALDO_INICIAL, 
            COALESCE(sf.FTF_SALDO_FINAL, 0) AS FTF_SALDO_FINAL
        FROM clientes c
        LEFT JOIN saldoinicial s ON c.FCN_CUENTA = s.FCN_CUENTA AND c.FCN_ID_TIPO_SUBCTA = s.FCN_ID_TIPO_SUBCTA 
        LEFT JOIN saldofinal sf ON c.FCN_CUENTA = sf.FCN_CUENTA AND c.FCN_ID_TIPO_SUBCTA = sf.FCN_ID_TIPO_SUBCTA
        ) 
        ,tablon as (
        SELECT 
            DISTINCT 
            ta.FCN_CUENTA AS FCN_CUENTA, 
            ta.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA, 
            COALESCE(ta.FTF_SALDO_INICIAL, 0) AS FTF_SALDO_INICIAL, 
            COALESCE(ta.FTF_SALDO_FINAL, 0) AS FTF_SALDO_FINAL, 
            COALESCE(ca.FTF_CARGO, 0) AS FTF_CARGO, 
            COALESCE(ab.FTF_ABONO, 0) AS FTF_ABONO, 
            COALESCE(cm.FTF_COMISION, 0) AS FTF_COMISION
        FROM saldos AS ta
            LEFT JOIN abono AS ab ON ta.FCN_CUENTA = ab.FCN_CUENTA AND ta.FCN_ID_TIPO_SUBCTA = ab.FCN_ID_TIPO_SUBCTA
            LEFT JOIN comision AS cm ON ta.FCN_CUENTA = cm.FCN_CUENTA AND ta.FCN_ID_TIPO_SUBCTA = cm.FCN_ID_TIPO_SUBCTA
            LEFT JOIN cargo AS ca ON ta.FCN_CUENTA = ca.FCN_CUENTA AND ta.FCN_ID_TIPO_SUBCTA = ca.FCN_ID_TIPO_SUBCTA
        )
        SELECT 
            DISTINCT 
            ta.FCN_CUENTA,
            {term_id} as FCN_ID_PERIODO,
            ta.FCN_ID_TIPO_SUBCTA,
            cast(ta.FTF_SALDO_INICIAL as numeric(16,2)) as FTF_SALDO_INICIAL,
            cast(ta.FTF_SALDO_FINAL as numeric(16,2)) as FTF_SALDO_FINAL,
            cast(ta.FTF_ABONO as numeric(16,2)) as FTF_ABONO,
            cast(ta.FTF_CARGO as numeric(16,2)) as FTF_CARGO,
            cast(ta.FTF_COMISION as numeric(16,2)) as FTF_COMISION,
            (ta.FTF_SALDO_FINAL - (ta.FTF_ABONO + ta.FTF_SALDO_INICIAL - ta.FTF_COMISION - ta.FTF_CARGO)) AS FTF_RENDIMIENTO_CALCULADO
        FROM tablon AS ta
        WHERE 1=1 and (ta.FTF_SALDO_FINAL - (ta.FTF_ABONO + ta.FTF_SALDO_INICIAL - ta.FTF_COMISION - ta.FTF_CARGO)) <> 0
        """)
        df.show(30)

        _write_spark_dataframe(df, configure_postgres_oci_spark, '"HECHOS"."TTCALCUL_RENDIMIENTO"')

        print("Se escribio rendimientos")


        # Cifras de control
        report1 = html_reporter.generate(
            postgres,
            """
            SELECT I."FTC_GENERACION" AS GENERACION,
                   I."FTC_VIGENCIA" AS VIGENCIA,
                   I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                   I."FTC_ORIGEN" AS ORIGEN,
                   COUNT(DISTINCT I."FCN_CUENTA") AS CLIENTES,
                   coalesce(ROUND(cast(SUM(R."FTF_SALDO_INICIAL") AS numeric(16,2)) ,2),0) AS SALDO_INICIAL,
                   coalesce(ROUND(cast(SUM(R."FTF_SALDO_FINAL") AS numeric(16,2)) ,2),0) AS SALDO_FINAL,
                   ROUND(cast(SUM(R."FTF_ABONO") AS numeric(16,2)) ,2) AS ABONO,
                   ROUND(cast(SUM(R."FTF_CARGO") AS numeric(16,2)) ,2) AS CARGO,
                   coalesce(ROUND(cast(SUM(R."FTF_COMISION") AS numeric(16,2)) ,2),0) AS COMISION,
                   ROUND(cast(SUM(R."FTF_RENDIMIENTO_CALCULADO") AS numeric(16,2)) ,2) AS RENDIMIENTO
            FROM "HECHOS"."TTCALCUL_RENDIMIENTO" R
            INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON R."FCN_CUENTA" = I."FCN_CUENTA" AND R."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
            --WHERE R."FCN_ID_PERIODO" = :term_id
            GROUP BY I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Cliente", "Indicador Afiliación"],
            ["Clientes", "SALDO_INICIAL", "SALDO_FINAL", "ABONO", "CARGO", "COMISION", "RENDIMIENTO"],
            params={"term_id": term_id},
        )
        report2 = html_reporter.generate(
            postgres,
            """
            SELECT I."FTC_GENERACION" AS GENERACION,
                   I."FTC_VIGENCIA" AS VIGENCIA,
                   I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                   I."FTC_ORIGEN" AS ORIGEN,
                   S."FTC_DESCRIPCION_CORTA" AS SIEFORE,
                   mp."FTC_DESCRIPCION" AS CONCEPTO,
                   SUM(C."FTF_MONTO_PESOS") AS MONTO_PESOS
            FROM "HECHOS"."TTHECHOS_MOVIMIENTO" C
                INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON C."FCN_CUENTA" = I."FCN_CUENTA" AND C."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
                INNER JOIN "MAESTROS"."TCDATMAE_SIEFORE" S ON C."FCN_ID_SIEFORE" = S."FTN_ID_SIEFORE"
                INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" MP ON MP."FTN_ID_MOVIMIENTO_PROFUTURO" = C."FCN_ID_CONCEPTO_MOVIMIENTO"
            WHERE C."FCN_ID_PERIODO" = :term_id
            GROUP BY  I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN", S."FTC_DESCRIPCION_CORTA", MP."FTC_DESCRIPCION"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SIEFORE", "Concepto"],
            ["MONTO_PESOS"],
            params={"term_id": term_id},
        )

        notify(
            postgres,
            f"Rendimientos",
            phase,
            area,
            term=term_id,
            message="Se han generado las cifras de control para Rendimientos exitosamente",
            details=report1,
        )
        notify(
            postgres,
            f"Rendimientos",
            phase,
            area,
            term=term_id,
            message="Se han generado las cifras de control para Rendimientos exitosamente",
            details=report2,
        )

        # Elimina tablas temporales
        postgres_oci.execute(text("""
                        DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"
                        """))


