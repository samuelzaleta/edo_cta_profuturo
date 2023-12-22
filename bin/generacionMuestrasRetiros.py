from profuturo.common import define_extraction, register_time, truncate_table, notify
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark, get_bigquery_pool
from profuturo.extraction import _write_spark_dataframe, extract_terms,  _get_spark_session, read_table_insert_temp_view
from pyspark.sql.functions import udf, concat, col, current_date , row_number,lit, current_timestamp
import sys
import requests


url = "https://procesos-api-service-dev-e46ynxyutq-uk.a.run.app/procesos/generarEstadosCuentaRetiros/muestras"

postgres_pool = get_postgres_pool()
bigquery_pool = get_bigquery_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool,bigquery_pool) as (postgres, bigquery):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        truncate_table(postgres, 'TCGESPRO_MUESTRA', term=term_id, area=area)
        truncate_table(bigquery, 'ESTADO_CUENTA.TTMUESTR_RETIRO_GENERAL')
        truncate_table(bigquery, 'ESTADO_CUENTA.TTMUESTR_RETIRO')

        read_table_insert_temp_view(configure_postgres_spark, """
                SELECT
                DISTINCT
                CA."FTC_USUARIO_CARGA",
                C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA"
                FROM "HECHOS"."TTHECHOS_RETIRO" F
                INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON F."FCN_CUENTA" = C."FTN_CUENTA"
                INNER JOIN "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA ON CA."FCN_CUENTA" = C."FTN_CUENTA" AND CA."FCN_ID_INDICADOR" = 32
                where C."FTN_CUENTA" is not null
                """, "user", params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        read_table_insert_temp_view(configure_postgres_spark, """
        SELECT
        DISTINCT
        C."FTN_CUENTA" as "FCN_ID_EDOCTA",
        C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA",
        :term AS "FCN_ID_PERIODO",
        concat_ws(' ', C."FTC_NOMBRE", C."FTC_AP_PATERNO", C."FTC_AP_MATERNO") AS "FTC_NOMBRE",
        C."FTC_CALLE" AS "FTC_CALLE_NUMERO",
        C."FTC_COLONIA",
        C."FTC_DELEGACION" AS "FTC_MUNICIPIO",
        Cast(C."FTN_CODIGO_POSTAL" as varchar ) AS "FTC_CP",
        C."FTC_ENTIDAD_FEDERATIVA" AS "FTC_ENTIDAD",
        C."FTC_CURP",
        C."FTC_RFC",
        C."FTC_NSS",
        --now() AS "FECHAHORA_ALTA",
        :user AS FTC_USUARIO_ALTA
        FROM "HECHOS"."TTHECHOS_RETIRO" F
        INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON F."FCN_CUENTA" = C."FTN_CUENTA"
        INNER JOIN "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA ON CA."FCN_CUENTA" = C."FTN_CUENTA" AND CA."FCN_ID_INDICADOR" = 32
        AND CA."FCN_ID_AREA" = :area
        where C."FTN_CUENTA" is not null
        and ca."FTC_USUARIO_CARGA" = :user
        """, "edoCtaGenerales", params={"term": term_id, "user": str(user), "area": area})
        general_df = spark.sql("""
        select * from edoCtaGenerales
        """)

        read_table_insert_temp_view(configure_postgres_spark,
          """
                SELECT
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
                CASE
                WHEN "FTC_FON_ENTIDAD" is not null then "FTN_SDO_TRA_VIVIENDA" + "FTN_SDO_TRA_AHORRORET"
                ELSE 0
                END FTN_MONTO_TRANSFERIDO,
                TO_CHAR("FTD_FECHA_EMISION",'YYYYMMDD') AS "FTD_FECHA_EMISION",
                "FTC_ENT_REC_TRAN",
                "FCC_MEDIO_PAGO",
                CASE
                WHEN "FTC_FON_ENTIDAD" is null then "FTN_SDO_TRA_VIVIENDA" + "FTN_SDO_TRA_AHORRORET" - "FTN_AFO_ISR"
                ELSE 0
                END "FTN_MONTO_TRANSFERIDO_AFORE",
                "FTN_AFO_ISR" AS "FTN_AFORE_RETENCION_ISR",
                "FTN_FEH_INI_PEN",
                "FTN_FEH_RES_PEN",
                "FTC_TIPO_TRAMITE",
                "FTN_ARCHIVO"
                FROM "HECHOS"."TTHECHOS_RETIRO" R
                INNER JOIN "HECHOS"."TTHECHOS_CARGA_ARCHIVO" CA ON CA."FCN_CUENTA" = R."FCN_CUENTA" AND CA."FCN_ID_INDICADOR" = 32
                AND "FCN_ID_AREA" = :area
                WHERE R."FCN_ID_PERIODO" = :term
                and ca."FTC_USUARIO_CARGA" = :user
                """, "edoCtaAnverso", params={"term": term_id, "user": str(user), "area": area})
        anverso_df = spark.sql("select * from edoCtaAnverso")


        _write_spark_dataframe(general_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_RETIRO_GENERAL')
        _write_spark_dataframe(anverso_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_RETIRO')

        df = spark.sql("""
        SELECT 
         FCN_NUMERO_CUENTA AS FCN_CUENTA,
         CAST(FTC_USUARIO_CARGA as int)  FTC_USUARIO_CARGA
        FROM user
        """)

        df.show()

        df = df.withColumn("FCN_ID_PERIODO", lit(term_id))
        df = df.withColumn("FCN_ID_USUARIO", col("FTC_USUARIO_CARGA").cast("int"))
        df = df.drop("FTC_USUARIO_CARGA")
        df = df.withColumn("FCN_ID_AREA", lit(area))
        df = df.withColumn("FTD_FECHAHORA_ALTA", lit(current_timestamp()))
        df = df.withColumn("FTC_URL_PDF_ORIGEN", concat(
            lit("https://storage.googleapis.com/edo_cuenta_profuturo_dev_b/profuturo-archivos/"),
            col("FCN_CUENTA"),
            lit(".pdf"),
        ))

        _write_spark_dataframe(df, configure_postgres_spark, '"GESTOR"."TCGESPRO_MUESTRA"')

        response = requests.get(url)

        # Verifica si la petición fue exitosa
        if response.status_code == 200:
            # Si la petición fue exitosa, puedes acceder al contenido de la respuesta de la siguiente manera:
            content = response.content
            print(content)
        else:
            # Si la petición no fue exitosa, puedes imprimir el código de estado para obtener más información
            print(f"La solicitud no fue exitosa. Código de estado: {response.status_code}")

        notify(
            postgres,
            "Generacion muestras retiros",
            phase,
            area,
            term=term_id,
            message="Se terminaron de generar las muestras de retiros de los estados de cuenta con éxito",
            aprobar=False,
            descarga=False,
        )
