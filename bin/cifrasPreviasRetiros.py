from profuturo.common import register_time, define_extraction, notify, truncate_table
from profuturo.database import get_postgres_pool, get_postgres_oci_pool, configure_postgres_spark, configure_mit_spark,configure_postgres_oci_spark,get_bigquery_pool
from profuturo.extraction import _get_spark_session, _write_spark_dataframe, read_table_insert_temp_view, upsert_dataset,extract_dataset_spark
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
import sys
from datetime import datetime

spark = _get_spark_session()


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_oci_pool ) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start = term["start_month"]
    end = term["end_month"]
    end_month_anterior = term["end_saldos_anterior"]
    valor_accion_anterior = term["valor_accion_anterior"]
    print(end_month_anterior, valor_accion_anterior)

    with register_time(postgres_pool, phase, term_id, user, area):
        truncate_table(postgres, "TTEDOCTA_CLIENTE_INDICADOR", term=term_id)

        upsert_dataset(postgres, postgres, """
        SELECT
        "FCN_CUENTA" fcn_cuenta,
        CA."FCN_ID_AREA" as fcn_id_area,
        "FCN_ID_INDICADOR" fcn_id_indicador,
        ARRAY[I."FTB_DISPONIBLE", I."FTB_ENVIO", I."FTB_IMPRESION", I."FTB_GENERACION"] AS fta_evalua_inidcador
        FROM
        "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA
            INNER JOIN
            "GESTOR"."TCGESPRO_INDICADOR" I ON CA."FCN_ID_INDICADOR" = I."FTN_ID_INDICADOR"
        WHERE
        "FCN_ID_PERIODO" = :term AND CA."FCN_ID_AREA" = :area AND
        ARRAY[I."FTB_DISPONIBLE", I."FTB_ENVIO", I."FTB_IMPRESION", I."FTB_GENERACION"]  <> '{true,true,true,true}'
        """, """
        INSERT INTO "TCHECHOS_CLIENTE_INDICADOR" ("FCN_CUENTA", "FCN_ID_AREA","FCN_ID_PERIODO", "FCN_ID_INDICADOR", "FTA_EVALUA_INDICADOR")
        VALUES (...)
        ON CONFLICT ("FCN_CUENTA", "FCN_ID_PERIODO", "FCN_ID_INDICADOR") DO UPDATE 
        SET "FTA_EVALUA_INDICADOR" = EXCLUDED."FTA_EVALUA_INDICADOR"
        """, lambda i: [f":fcn_cuenta_{i}", f":fcn_id_area_{i}", f"{term_id}", f":fcn_id_indicador_{i}",
                        f":fta_evalua_inidcador_{i}"],
       "TCHECHOS_CLIENTE_INDICADOR", select_params={'term': term_id, 'area': area})

        extract_dataset_spark(configure_postgres_spark, configure_postgres_spark, """
        SELECT
        "FCN_CUENTA",
        "FCN_ID_PERIODO",
        :area AS "FCN_ID_AREA",
        MIN("FTB_ENVIO")::bool AS "FTB_ENVIO",
        MIN("FTB_IMPRESION")::bool AS "FTB_IMPRESION",
        MIN("FTB_DISPONIBLE")::bool AS "FTB_DISPONIBLE",
        MIN("FTB_GENERACION")::bool AS "FTB_GENERACION"
        FROM (
        SELECT "FCN_CUENTA", "FCN_ID_PERIODO", "FCN_ID_INDICADOR",
                ("FTA_EVALUA_INDICADOR"[1])::int AS "FTB_DISPONIBLE",
                ("FTA_EVALUA_INDICADOR"[2])::int AS "FTB_ENVIO",
               ("FTA_EVALUA_INDICADOR"[3])::int AS "FTB_IMPRESION",
               ("FTA_EVALUA_INDICADOR"[4])::int AS "FTB_GENERACION"
        FROM "HECHOS"."TCHECHOS_CLIENTE_INDICADOR"
        WHERE "FCN_ID_PERIODO" = :term
        ) X
        GROUP BY
        "FCN_CUENTA",
        "FCN_ID_PERIODO"
        """, '"ESTADO_CUENTA"."TTEDOCTA_CLIENTE_INDICADOR"', params={'term': term_id, 'area': area})

        query = """
            SELECT FTC_ENTIDAD_FEDERATIVA, INDICADOR, COUNT(1) NUM_REGISTROS
            FROM (
                SELECT COALESCE(MC."FTC_ENTIDAD_FEDERATIVA", 'NO ASIGNADO') AS FTC_ENTIDAD_FEDERATIVA,
                       GI."FTC_DESCRIPCION" AS INDICADOR 
                FROM "HECHOS"."TCHECHOS_CLIENTE_INDICADOR" CI
                    INNER JOIN "GESTOR"."TCGESPRO_INDICADOR" GI ON GI."FTN_ID_INDICADOR" = CI."FCN_ID_INDICADOR"
                    LEFT JOIN "MAESTROS"."TCDATMAE_CLIENTE" MC ON CI."FCN_CUENTA" = MC."FTN_CUENTA"
                WHERE CI."FCN_ID_PERIODO" = :term
            ) I
            GROUP BY FTC_ENTIDAD_FEDERATIVA, INDICADOR
        """

        read_table_insert_temp_view(
            configure_postgres_spark,
            query,
            "INDICADOR",
            params={"term": term_id}
        )
        df = spark.sql("SELECT * FROM INDICADOR")

        # Convert PySpark DataFrame to pandas DataFrame
        pandas_df = df.toPandas()

        # Convert pandas DataFrame to HTML
        html_table = pandas_df.to_html()

        # Enviar notificación con la tabla HTML de este lote

        notify(
            postgres,
            f"Indicadores",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para indicadores exitosamente para el periodo",
            details=html_table,
            visualiza=False
        )

        query = """
        select
        R."FCN_CUENTA",
        "FTC_FOLIO",
        "FTN_SDO_INI_AHORRORET",
        "FTN_SDO_INI_VIVIENDA",
        "FTN_SDO_TRA_AHORRORET",
        "FTN_SDO_TRA_VIVIENDA",
        "FTN_SDO_INI_AHORRORET" - "FTN_SDO_TRA_AHORRORET" AS "FTN_SALDO_REM_AHORRORET",
        "FTN_SDO_INI_VIVIENDA" - "FTN_SDO_TRA_VIVIENDA" AS "FTN_SALDO_REM_VIVIENDA",
        "FTC_LEY_PENSION",
        "FTC_REGIMEN",
        "FTC_TPSEGURO",
        "FTC_TPPENSION",
        "FTC_FON_ENTIDAD",
        case
        when "FTC_FON_ENTIDAD" is not null then "FTN_SDO_TRA_VIVIENDA" + "FTN_SDO_TRA_AHORRORET"
            else 0
            end FTN_MONTO_TRANSFERIDO,
        TO_CHAR("FTD_FECHA_EMISION",'YYYYMMDD') AS "FTD_FECHA_EMISION",
        --0 AS FTN_RECURSO_RETENCION_ISR,
        "FTC_ENT_REC_TRAN",
        "FCC_MEDIO_PAGO",
        case
        when "FTC_FON_ENTIDAD" is null then "FTN_SDO_TRA_VIVIENDA" + "FTN_SDO_TRA_AHORRORET" - "FTN_AFO_ISR"
            else 0
            end "FTN_MONTO_TRANSFERIDO_AFORE",
        "FTN_AFO_ISR" AS "FTN_AFORE_RETENCION_ISR",
        "FTN_FEH_INI_PEN",
        "FTN_FEH_RES_PEN",
        "FTC_TIPO_TRAMITE",
        "FTN_ARCHIVO"
        from "HECHOS"."TTHECHOS_RETIRO" R
        where R."FCN_ID_PERIODO" = :term
        """

        read_table_insert_temp_view(configure_postgres_oci_spark, query, "retiros", params={"term": term_id, "user": str(user), "area": area})
        df = spark.sql(""" select * from retiros""")
        # Convert PySpark DataFrame to pandas DataFrame
        pandas_df = df.toPandas()

        # Convert pandas DataFrame to HTML
        html_table = pandas_df.to_html()
        notify(
            postgres,
            f"CIFRAS PREVIAS RETIROS",
            phase,
            area,
            term=term_id,
            message="Se han generado las cifras de previas exitosamente",
            details=html_table,
        )

