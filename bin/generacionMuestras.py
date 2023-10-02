from profuturo.common import define_extraction, register_time, truncate_table
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms, extract_dataset_spark, _get_spark_session, read_table_insert_temp_view
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import udf, concat, col, current_date , row_number,lit, current_timestamp
from pyspark.sql.window import Window
import uuid
import sys

postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase=phase, area=area, usuario=user, term=term_id):
        read_table_insert_temp_view(configure_postgres_spark, """
        SELECT DISTINCT F."FCN_ID_GENERACION" AS "FTN_ID_GRUPO_SEGMENTACION",
               -- F."FCN_ID_GENERACION",
               'CANDADO' AS "FTC_CANDADO_APERTURA",
               F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
               M."FCN_ID_PERIODO",
               replace(P."FTC_PERIODO", '/','') AS "PERIODO",
               C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA",
               :end AS "FTD_FECHA_CORTE",
               :start - INTERVAL '1 month' * (PR."FTN_MESES" - 1) AS "FTD_FECHA_GRAL_INICIO",
               :end AS "FTD_FECHA_GRAL_FIN",
               :start - INTERVAL '1 month' * (PA."FTN_MESES" - 1) AS "FTD_FECHA_MOV_INICIO",
               :end AS "FTD_FECHA_MOV_FIN",
               0 AS "FTN_ID_SIEFORE",
               cast(I."FTC_PERFIL_INVERSION" AS varchar) AS "FTC_DESC_SIEFORE",
               cast(I."FTC_GENERACION" AS varchar) AS "FTC_DESC_TIPOGENERACION",
               concat_ws(' ', C."FTC_NOMBRE", C."FTC_AP_PATERNO", C."FTC_AP_MATERNO") AS "FTC_NOMBRE_COMPLETO",
               C."FTC_CALLE" AS "FTC_CALLE_NUMERO",
               C."FTC_COLONIA",
               C."FTC_DELEGACION",
               C."FTN_CODIGO_POSTAL" AS "FTN_CP",
               C."FTC_ENTIDAD_FEDERATIVA",
               C."FTC_NSS",
               C."FTC_RFC",
               C."FTC_CURP",
               now() AS "FTD_FECHAHORA_ALTA",
               '0' AS "FTC_USUARIO_ALTA"
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PR ON F."FCN_ID_PERIODICIDAD_REVERSO" = PR."FTN_ID_PERIODICIDAD"
            INNER JOIN  "HECHOS"."TCHECHOS_CLIENTE" I ON I."FCN_ID_PERIODO" = 1 /* :term */ AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
            INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IE ON IE."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_CLIENTE" AND IE."FTN_VALOR" = CASE I."FTC_ORIGEN" WHEN 'ISSSTE' THEN  67 WHEN 'IMSS' THEN 66 WHEN 'MIXTO' THEN 69 WHEN 'INDEPENDIENTE' THEN 68 END
            INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IEC ON IEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_AFILIACION" AND IEC."FTN_VALOR" = CASE  WHEN I."FTC_TIPO_CLIENTE" = 'Afiliado' THEN  714 WHEN I."FTC_TIPO_CLIENTE" = 'Asignado' THEN 713 WHEN I."FTB_PENSION" = true THEN 1 END
            -- INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" TIEC ON TIEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_BONO" AND TIEC."FTN_VALOR" = CASE I."FTB_BONO" WHEN false THEN  1 WHEN true THEN 2 END
            INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON I."FCN_CUENTA" = C."FTN_CUENTA"
            INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" M ON C."FTN_CUENTA" = M."FCN_CUENTA"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODO" P ON P."FTN_ID_PERIODO" = 1 -- :term
        WHERE F."FTB_ESTATUS" = true
          AND mod(extract(MONTH FROM to_date(P."FTC_PERIODO", 'MM/YYYY')), PA."FTN_MESES") = 0
          AND mod(extract(MONTH FROM to_date(P."FTC_PERIODO", 'MM/YYYY')), PR."FTN_MESES") = 0
        """, "edoCtaGenerales", params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        general_df = spark.sql("select * from edoCtaGenerales")
        general_df = general_df.withColumn("FCN_ID_EDOCTA", concat(
            col("FCN_NUMERO_CUENTA"),
            col("PERIODO"),
            col("FTN_ID_FORMATO"),
        ).cast("bigint"))
        general_df = general_df.withColumn("FCN_FOLIO", row_number().over(Window.orderBy(lit(0))).cast("string"))
        general_df = general_df.drop(col("PERIODO"))
        general_df.createOrReplaceTempView("general")

        read_table_insert_temp_view(configure_postgres_spark, """
        SELECT -- F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FCN_ID_EDOCTA",
               REPLACE(PR."FTC_PERIODO", '/','') AS "PERIODO",
               F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
               MC."FTC_MOV_TIPO_AHORRO" AS "FTC_SECCION",
               R."FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
               R."FTD_FEH_LIQUIDACION" AS "FTD_FECHA_MOVIMIENTO",
               MC."FTN_ID_MOVIMIENTO_CONSAR" AS "FTN_ID_CONCEPTO",
               MC."FTC_DESCRIPCION" AS "FTC_DESC_CONCEPTO",
               'Desconocido' AS "FTC_PERIODO_REFERENCIA",
               sum(R."FTF_MONTO_PESOS") AS "FTN_MONTO",
               0 AS "FTN_DIA_COTIZADO",
               cast(0.0 as numeric) as "FTN_SALARIO_BASE",
               now() AS "FTD_FECHAHORA_ALTA",
               cast(:user as varchar) AS "FTC_USUARIO_ALTA"
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PR ON F."FCN_ID_PERIODICIDAD_REVERSO" = PR."FTN_ID_PERIODICIDAD"
            INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON I."FCN_ID_PERIODO" = :term --AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
            INNER JOIN "HECHOS"."TTHECHOS_MOVIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
            INNER JOIN "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON R."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
            INNER JOIN "MAESTROS"."TCDATMAE_MOVIMIENTO_CONSAR" MC ON PC."FCN_ID_MOVIMIENTO_CONSAR" = MC."FTN_ID_MOVIMIENTO_CONSAR"
            INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" MU ON MU."FCN_CUENTA" = R."FCN_CUENTA"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODO" PR ON PR."FTN_ID_PERIODO" = 1 --:term
            --INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" PC ON R."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FTN_ID_MOVIMIENTO_PROFUTURO"
            --INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA" AND I."FCN_ID_PERIODO" = R."FCN_ID_PERIODO"
        WHERE mod(extract(MONTH FROM to_date(T."FTC_PERIODO", 'MM/YYYY')), PA."FTN_MESES") = 0
          AND mod(extract(MONTH FROM to_date(T."FTC_PERIODO", 'MM/YYYY')), PR."FTN_MESES") = 0
          AND to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * (P."FTN_MESES" - 1) AND :end
        GROUP BY  F."FCN_ID_FORMATO_ESTADO_CUENTA", PR."FTC_PERIODO", F."FCN_ID_FORMATO_ESTADO_CUENTA", R."FCN_CUENTA", MC."FTN_ID_MOVIMIENTO_CONSAR", R."FTD_FEH_LIQUIDACION"
        """, "edoCtaReverso", params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        reverso_df = spark.sql("select  * from edoCtaReverso")
        """
        reverso_df = reverso_df.withColumn("FCN_ID_EDOCTA", concat(
            col("FCN_NUMERO_CUENTA"),
            col("PERIODO"),
            col("FTN_ID_FORMATO"),
        ).cast("bigint"))
        """
        reverso_df = reverso_df.drop(col("PERIODO"))
        reverso_df = reverso_df.drop(col("FTN_ID_FORMATO"))
        reverso_df.createOrReplaceTempView("reverso")

        read_table_insert_temp_view(configure_postgres_spark, """
        WITH SaldoIni as (
            select T."FTN_ID_PERIODO",
                   min(to_date(T."FTC_PERIODO", 'MM/YYYY'))
            FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
                INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON :term = I."FCN_ID_PERIODO" AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
                INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
                INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" MU on MU."FCN_CUENTA" = R."FCN_CUENTA"
            WHERE to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * P."FTN_MESES" AND :end
            GROUP BY 1
        ), SaldoFin AS (
            select T."FTN_ID_PERIODO",
                   max(to_date(T."FTC_PERIODO", 'MM/YYYY'))
            FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
                INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON :term = I."FCN_ID_PERIODO" AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
                INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
                INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" MU on MU."FCN_CUENTA" = R."FCN_CUENTA"
            WHERE to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * P."FTN_MESES" AND :end
            GROUP BY 1
        ), dataset AS (
            select C."FCN_GENERACION",
                   I."FCN_CUENTA",
                   C."FTC_AHORRO",
                   C."FTC_DES_CONCEPTO",
                   C."FTC_SECCION",
                   replace(PR."FTC_PERIODO", '/','') AS "PERIODO",
                   F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
                   sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_ABONO" ELSE 0 END) AS aportaciones,
                   sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_CARGO" ELSE 0 END) AS retiros,
                   -- sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_RENDIMIENTO_CALCULADO" ELSE 0 END) AS rendimientos,
                   sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_COMISION" ELSE 0 END) AS comisiones,
                   sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" =  ANY(C."FTA_SUBCUENTAS") AND R."FCN_ID_PERIODO" = (Select I."FTN_ID_PERIODO" from SaldoIni I) THEN R."FTF_SALDO_INICIAL" ELSE 0 END) AS saldoInicial,
                   sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" =  ANY(C."FTA_SUBCUENTAS") AND R."FCN_ID_PERIODO" = (Select I."FTN_ID_PERIODO" from SaldoFin I) THEN R."FTF_SALDO_FINAL" ELSE 0 END) AS saldoFinal,
                   :start - INTERVAL '1 month' * P."FTN_MESES" AS "FTD_FECHA_INICIO",
                   :end AS "FTD_FECHA_FINAL"
            FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
                INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON :term = I."FCN_ID_PERIODO" AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
                INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
                inner join "GESTOR"."TCGESPRO_MUESTRA" MU on MU."FCN_CUENTA" = R."FCN_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" PR ON PR."FTN_ID_PERIODO" = :term
            WHERE mod(extract(MONTH FROM :end), P."FTN_MESES") = 0
              -- AND CURRENT_DATE() BETWEEN to_date(CF."FTD_INICIO_VIGENCIA", 'MM/YYYY') AND coalesce(date_trunc(to_date("FTD_FIN_VIGENCIA", 'MM/YYYY')), now())
            GROUP BY I."FCN_CUENTA", C."FTC_AHORRO", C."FTC_DES_CONCEPTO", C."FTC_SECCION", 
                     F."FCN_ID_FORMATO_ESTADO_CUENTA", PR."FTC_PERIODO", C."FCN_GENERACION"
        )
        SELECT "FCN_CUENTA" as "FCN_NUMERO_CUENTA",
               "PERIODO",
               "FTN_ID_FORMATO",
               "FTC_DES_CONCEPTO" as "FTC_CONCEPTO_NEGOCIO",
               cast(aportaciones as numeric(16,2)) as "FTF_APORTACION",
               cast(retiros as numeric(16,2)) as "FTN_RETIRO",
               cast(comisiones as numeric(16,2)) as "FTN_COMISION",
               cast(saldoInicial as numeric(16,2)) as "FTN_SALDO_ANTERIOR",
               cast(saldoFinal as numeric(16,2))  as "FTN_SALDO_FINAL",
               cast((saldoFinal - (aportaciones + saldoInicial - comisiones - retiros)) as numeric(16,2)) AS "FTN_RENDIMIENTO",
               cast(0.0 as numeric(16,2)) AS "FTN_VALOR_ACTUAL_PESO",
               cast(0.000 as numeric(16,6)) AS "FTN_VALOR_ACTUAL_UDI",
               cast(0.0 as numeric(16,2))  AS "FTN_VALOR_NOMINAL_PESO",
               cast(0.000 as numeric(16,6)) AS "FTN_VALOR_NOMINAL_UDI",
               1 AS "FTN_TIPO_PENSION",
               cast(0.0 as numeric(16,2)) AS "FTN_MONTO_PENSION",
               "FTC_SECCION" AS  "FTC_SECCION",
               "FTC_AHORRO" AS "FTC_TIPO_AHORRO",
               now() AS "FTD_FECHAHORA_ALTA",
               '0' AS "FTC_USUARIO_ALTA",
               "FTD_FECHA_INICIO",
               "FTD_FECHA_FINAL"
        FROM dataset
        """, "edoCtaAnverso", params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)})
        anverso_df = spark.sql("select * from edoCtaAnverso")
        """
        anverso_df = anverso_df.withColumn("FCN_ID_EDOCTA", concat(
            col("FCN_NUMERO_CUENTA"),
            col("PERIODO"),
            col("FTN_ID_FORMATO"),
        ).cast("bigint"))
        """
        anverso_df = anverso_df.drop(col("PERIODO"))
        anverso_df = anverso_df.drop(col("FTN_ID_FORMATO"))
        anverso_df.createOrReplaceTempView("anverso")

        general_df = spark.sql("""
        SELECT C.*, ASS.FTN_MONTO AS FTN_SALDO_SUBTOTAL, AST.FTN_MONTO AS FTN_SALDO_TOTAL
        FROM general C
        INNER JOIN (
            SELECT FCN_NUMERO_CUENTA, SUM(FTN_MONTO) AS FTN_MONTO
            FROM anverso
            WHERE FTC_SECCION = 'AHO'
            GROUP BY FCN_NUMERO_CUENTA
        ) ASS ON C.FCN_NUMERO_CUENTA = ASS.FCN_NUMERO_CUENTA
        INNER JOIN (
            SELECT FCN_NUMERO_CUENTA, SUM(FTN_MONTO) AS FTN_MONTO
            FROM anverso
            GROUP BY FCN_NUMERO_CUENTA
        ) AST ON C.FCN_NUMERO_CUENTA = AST.FCN_NUMERO_CUENTA
        """)

        reverso_df = spark.sql("""
        SELECT R.*, C.FCN_ID_EDOCTA
        FROM reverso R
            INNER JOIN general C ON C.FCN_NUMERO_CUENTA = R.FCN_NUMERO_CUENTA
        WHERE R.FCN_NUMERO_CUENTA IN (SELECT FCN_NUMERO_CUENTA FROM anverso)
        """)

        anverso_df = spark.sql("""
        SELECT R.*, C.FCN_ID_EDOCTA 
        FROM anverso R
        INNER JOIN general C ON C.FCN_NUMERO_CUENTA = R.FCN_NUMERO_CUENTA
        """)

        read_table_insert_temp_view(configure_postgres_spark, """
        SELECT "FTN_ID_MUESTRA",
               "FCN_CUENTA",
               "FCN_ID_PERIODO",
               -- "FTC_URL_PDF_ORIGEN",
               "FTC_ESTATUS",
               "FCN_ID_USUARIO",
               "FCN_ID_AREA",
               "FTD_FECHAHORA_ALTA"
        FROM "GESTOR"."TCGESPRO_MUESTRA"
        WHERE "FCN_ID_PERIODO" = :term
        """, "muestras", params={"term": term_id, "user": str(user)})

        df = spark.sql("""
        SELECT -- m.FTN_ID_MUESTRA,
               m.FCN_CUENTA,
               m.FCN_ID_PERIODO,
               m.FTC_ESTATUS,
               34 as FCN_ID_USUARIO,
               cast(m.FCN_ID_AREA as INTEGER) as FCN_ID_AREA
        FROM muestras m
        """)

        df = df.withColumn("FTD_FECHAHORA_ALTA", lit(current_timestamp()))
        # Considera inner join del periodo
        df = df.join(general_df, df.FCN_CUENTA == general_df.FCN_NUMERO_CUENTA, "right") \
            .select(
                df.FCN_CUENTA, df.FCN_ID_PERIODO, df.FTC_ESTATUS, df.FCN_ID_USUARIO, df.FCN_ID_AREA,
                df.FTD_FECHAHORA_ALTA, general_df.FCN_FOLIO,
            )

        df = df.withColumn("FTC_URL_PDF_ORIGEN", concat(
            lit("https://storage.googleapis.com/profuturo-archivos/"),
            col("FCN_FOLIO"),
            lit(".pdf")
        ))
        df = df.drop(col("FCN_FOLIO"))

        truncate_table(postgres, "TCGESPRO_MUESTRA_SOL_RE_CONSAR")

        _write_spark_dataframe(reverso_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_REVERSO')
        _write_spark_dataframe(anverso_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_ANVERSO')
        _write_spark_dataframe(general_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_GENERAL')
        _write_spark_dataframe(df, configure_postgres_spark, '"GESTOR"."TCGESPRO_MUESTRA"')
