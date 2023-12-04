from profuturo.common import define_extraction, register_time, notify, truncate_table
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark, get_bigquery_pool
from profuturo.extraction import _write_spark_dataframe, extract_terms, _get_spark_session, _create_spark_dataframe
from pyspark.sql.functions import concat, col, row_number, lit, lpad
from pyspark.sql.window import Window
import sys
import random
import string


postgres_pool = get_postgres_pool()
bigquery_pool = get_bigquery_pool()
phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, bigquery_pool) as (postgres, bigquery):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        truncate_table(bigquery, 'ESTADO_CUENTA.TTMUESTR_REVERSO')
        truncate_table(bigquery, 'ESTADO_CUENTA.TTMUESTR_ANVERSO')
        truncate_table(bigquery, 'ESTADO_CUENTA.TTMUESTR_GENERAL')

        char1 = random.choice(string.ascii_letters).upper()
        char2 = random.choice(string.ascii_letters).upper()
        random = char1 + char2
        print(random)

        general_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        SELECT DISTINCT F."FCN_ID_GENERACION" AS "FTN_ID_GRUPO_SEGMENTACION",
               -- F."FCN_ID_GENERACION",
               'CANDADO' AS "FTC_CANDADO_APERTURA",
               F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
               :term AS "FCN_ID_PERIODO",
               --CONCAT(SUBSTR(P."FTC_PERIODO", 4), SUBSTR(P."FTC_PERIODO", 1, 2)) AS "PERIODO",
               C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA",
               CAST(:end as TIMESTAMP) AS "FTD_FECHA_CORTE",
               CAST(:end - INTERVAL '1 month' * (PR."FTN_MESES" - 1) AS TIMESTAMP) AS "FTD_FECHA_GRAL_INICIO",
               CAST(:end as TIMESTAMP)  AS "FTD_FECHA_GRAL_FIN",
               CAST(:end as TIMESTAMP)  - INTERVAL '1 month' * (PA."FTN_MESES" - 1) AS "FTD_FECHA_MOV_INICIO",
               CAST(:end as TIMESTAMP)  AS "FTD_FECHA_MOV_FIN",
               0 AS "FTN_ID_SIEFORE",
               '55-60' AS "FTC_DESC_SIEFORE",
               cast(I."FTC_GENERACION" AS varchar) AS "FTC_TIPOGENERACION",
               FE."FTC_DESC_GENERACION" AS FTC_DESC_TIPOGENERACION,
               concat_ws(' ', C."FTC_NOMBRE", C."FTC_AP_PATERNO", C."FTC_AP_MATERNO") AS "FTC_NOMBRE_COMPLETO",
               C."FTC_CALLE" AS "FTC_CALLE_NUMERO",
               C."FTC_COLONIA",
               C."FTC_DELEGACION",
               C."FTN_CODIGO_POSTAL" AS "FTN_CP",
               C."FTC_ENTIDAD_FEDERATIVA",
               C."FTC_NSS",
               C."FTC_RFC",
               C."FTC_CURP",
               -- now() AS "FTD_FECHAHORA_ALTA",
               :user AS "FTC_USUARIO_ALTA"
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
            INNER JOIN "GESTOR"."TCGESPRO_FORMATO_ESTADO_CUENTA" FE ON F."FCN_ID_FORMATO_ESTADO_CUENTA" = FE."FTN_ID_FORMATO_ESTADO_CUENTA"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_GENERACION" = PG."FTN_ID_PERIODICIDAD"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PR ON F."FCN_ID_PERIODICIDAD_REVERSO" = PR."FTN_ID_PERIODICIDAD"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODO" P ON P."FTN_ID_PERIODO" = :term
            INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
                ON F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
                   WHEN 'AFORE' THEN 2
                   WHEN 'TRANSICION' THEN 3
                   WHEN 'MIXTO' THEN 4
               END
            INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON I."FCN_CUENTA" = C."FTN_CUENTA"
            INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IE
                ON IE."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_CLIENTE"
               AND IE."FTN_VALOR" = CASE I."FTC_ORIGEN"
                   WHEN 'ISSSTE' THEN  67
                   WHEN 'IMSS' THEN 66
                   WHEN 'MIXTO' THEN 69
                   WHEN 'INDEPENDIENTE' THEN 68
               END
            INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IEC
                ON IEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_AFILIACION"
               AND IEC."FTN_VALOR" = CASE
                   WHEN I."FTB_PENSION" THEN 1
                   WHEN I."FTC_TIPO_CLIENTE" = 'Afiliado' THEN 714
                   WHEN I."FTC_TIPO_CLIENTE" = 'Asignado' THEN 713
               END
            INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" TIEC
                ON TIEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_BONO"
               AND TIEC."FTN_VALOR" = CASE I."FTB_BONO"
                   WHEN false THEN 1
                   WHEN true THEN 2
               END
        -- QUITAR COMMENT CONDICION VALIDA PERO NO ES PERIODO CUATRIMESTRAL
        WHERE mod(extract(MONTH FROM to_date(P."FTC_PERIODO", 'MM/YYYY')), PG."FTN_MESES") = 0
          AND F."FTB_ESTATUS" = true
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        general_df = general_df.withColumn("FCN_ID_EDOCTA", concat(
            col("FCN_NUMERO_CUENTA"),
            col("FCN_ID_PERIODO"),
            col("FTN_ID_FORMATO"),
        ).cast("bigint"))
        general_df = general_df.withColumn("consecutivo", row_number().over(Window.orderBy(lit(0))).cast("string"))
        # Fill the "consecutivo" column with 9-digit values
        general_df = general_df.withColumn("consecutivo", lpad("consecutivo", 9, '0'))
        general_df = general_df.withColumn("FCN_FOLIO", concat(
            lit(random),
            col("FCN_ID_PERIODO"),
            col("FTN_ID_FORMATO"),
            col("consecutivo"),
        ))
        general_df = general_df.drop(col("consecutivo"))
        general_df.createOrReplaceTempView("general")

        reverso_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        WITH dataset as (
            SELECT DISTINCT F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO", C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA"
            FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                INNER JOIN "GESTOR"."TCGESPRO_FORMATO_ESTADO_CUENTA" FE ON F."FCN_ID_FORMATO_ESTADO_CUENTA" = FE."FTN_ID_FORMATO_ESTADO_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_GENERACION" = PG."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PR ON F."FCN_ID_PERIODICIDAD_REVERSO" = PR."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" P ON P."FTN_ID_PERIODO" = :term
                INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
                    ON F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
                       WHEN 'AFORE' THEN 2
                       WHEN 'TRANSICION' THEN 3
                       WHEN 'MIXTO' THEN 4 
                   END
                INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON I."FCN_CUENTA" = C."FTN_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IE
                    ON IE."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_CLIENTE"
                   AND IE."FTN_VALOR" = CASE I."FTC_ORIGEN"
                       WHEN 'ISSSTE' THEN 67
                       WHEN 'IMSS' THEN 66
                       WHEN 'MIXTO' THEN 69
                       WHEN 'INDEPENDIENTE' THEN 68 
                   END
                INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IEC
                    ON IEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_AFILIACION"
                   AND IEC."FTN_VALOR" = CASE
                       WHEN I."FTB_PENSION" THEN 1
                       WHEN I."FTC_TIPO_CLIENTE" = 'Afiliado' THEN 714
                       WHEN I."FTC_TIPO_CLIENTE" = 'Asignado' THEN 713
                   END
                INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" TIEC
                    ON TIEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_BONO"
                   AND TIEC."FTN_VALOR" = CASE I."FTB_BONO"
                       WHEN false THEN  1
                       WHEN true THEN 2 
                   END
            -- QUITAR COMENT  CONDICION VALIDA PERO NO ES PERIODO CUATRIMESTRAL
            WHERE mod(extract(MONTH FROM to_date(P."FTC_PERIODO", 'MM/YYYY')), PG."FTN_MESES") = 0
              AND F."FTB_ESTATUS" = true
        )
        SELECT --PRD."FTC_PERIODO",
               F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
               MC."FTC_MOV_TIPO_AHORRO" AS "FTC_SECCION",
               R."FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
               R."FTD_FEH_LIQUIDACION" AS "FTD_FECHA_MOVIMIENTO",
               MC."FTN_ID_MOVIMIENTO_CONSAR" AS "FTN_ID_CONCEPTO",
               MC."FTC_DESCRIPCION" AS "FTC_DESC_CONCEPTO",
               NULL AS "FTC_PERIODO_REFERENCIA",
               sum(R."FTF_MONTO_PESOS") AS "FTN_MONTO",
               0 AS "FTN_DIA_COTIZADO",
               cast(0.0 as numeric) as "FTN_SALARIO_BASE",
               -- now() AS "FTD_FECHAHORA_ALTA",
               :user AS FTC_USUARIO_ALTA
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_REVERSO" = PG."FTN_ID_PERIODICIDAD"
            INNER JOIN dataset D ON D."FTN_ID_FORMATO" = F."FCN_ID_FORMATO_ESTADO_CUENTA"
            INNER JOIN "HECHOS"."TTHECHOS_MOVIMIENTO" R ON D."FCN_NUMERO_CUENTA" = R."FCN_CUENTA"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
            INNER JOIN "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON R."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
            INNER JOIN "MAESTROS"."TCDATMAE_MOVIMIENTO_CONSAR" MC ON PC."FCN_ID_MOVIMIENTO_CONSAR" = MC."FTN_ID_MOVIMIENTO_CONSAR"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODO" PRD ON PRD."FTN_ID_PERIODO" = :term
        WHERE mod(extract(MONTH FROM to_date(T."FTC_PERIODO", 'MM/YYYY')), PG."FTN_MESES") = 0
          AND to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * (PG."FTN_MESES" - 1) AND :end
        GROUP BY F."FCN_ID_FORMATO_ESTADO_CUENTA",
                 PRD."FTC_PERIODO",
                 F."FCN_ID_FORMATO_ESTADO_CUENTA",
                 R."FCN_CUENTA",
                 MC."FTN_ID_MOVIMIENTO_CONSAR",
                 R."FTD_FEH_LIQUIDACION"
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        # reverso_df = reverso_df.drop(col("PERIODO"))
        reverso_df = reverso_df.drop(col("FTN_ID_FORMATO"))
        reverso_df.createOrReplaceTempView("reverso")

        anverso_df = _create_spark_dataframe(spark, configure_postgres_spark, f"""
        WITH dataset AS (
            SELECT DISTINCT F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
                   C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA"
            FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                INNER JOIN "GESTOR"."TCGESPRO_FORMATO_ESTADO_CUENTA" FE ON F."FCN_ID_FORMATO_ESTADO_CUENTA" = FE."FTN_ID_FORMATO_ESTADO_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_GENERACION" = PG."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PA ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PA."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PR ON F."FCN_ID_PERIODICIDAD_REVERSO" = PR."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" P ON P."FTN_ID_PERIODO" = :term
                INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
                    ON F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION"
                       WHEN 'AFORE' THEN 2
                       WHEN 'TRANSICION' THEN 3
                       WHEN 'MIXTO' THEN 4
                   END
                INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C ON I."FCN_CUENTA" = C."FTN_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IE
                    ON IE."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_CLIENTE"
                   AND IE."FTN_VALOR" = CASE I."FTC_ORIGEN"
                       WHEN 'ISSSTE' THEN 67
                       WHEN 'IMSS' THEN 66
                       WHEN 'MIXTO' THEN 69
                       WHEN 'INDEPENDIENTE' THEN 68
                   END
                INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" IEC
                    ON IEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_AFILIACION"
                   AND IEC."FTN_VALOR" = CASE
                       WHEN I."FTB_PENSION" THEN 1
                       WHEN I."FTC_TIPO_CLIENTE" = 'Afiliado' THEN 714
                       WHEN I."FTC_TIPO_CLIENTE" = 'Asignado' THEN 713
                   END
                INNER JOIN "GESTOR"."TCGESPRO_INDICADOR_ESTADO_CUENTA" TIEC
                    ON TIEC."FTN_ID_INDICADOR_ESTADO_CUENTA" = F."FCN_ID_INDICADOR_BONO"
                   AND TIEC."FTN_VALOR" = CASE I."FTB_BONO"
                       WHEN false THEN 1
                       WHEN true THEN 2
                   END
            -- QUITAR COMMENT CONDICION VALIDA PERO NO ES PERIODO CUATRIMESTRAL
            WHERE mod(extract(MONTH FROM to_date(P."FTC_PERIODO", 'MM/YYYY')), PG."FTN_MESES") = 0
              AND F."FTB_ESTATUS" = true
        ), SaldoIni AS (
            SELECT T."FTN_ID_PERIODO",
                   min(to_date(T."FTC_PERIODO", 'MM/YYYY'))
            FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PG."FTN_ID_PERIODICIDAD"
                INNER JOIN dataset D ON d."FTN_ID_FORMATO" = F."FCN_ID_FORMATO_ESTADO_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
                INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON d."FCN_NUMERO_CUENTA" = R."FCN_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
            WHERE to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * PG."FTN_MESES" AND :end
              AND mod(extract(MONTH FROM :end), P."FTN_MESES") = 0
            GROUP BY T."FTN_ID_PERIODO"
        ), SaldoFin AS (
            SELECT T."FTN_ID_PERIODO",
                   max(to_date(T."FTC_PERIODO", 'MM/YYYY'))
            FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PG."FTN_ID_PERIODICIDAD"
                INNER JOIN dataset D ON D."FTN_ID_FORMATO" = F."FCN_ID_FORMATO_ESTADO_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
                INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON d."FCN_NUMERO_CUENTA" = R."FCN_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
            WHERE to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * PG."FTN_MESES" AND :end
            GROUP BY T."FTN_ID_PERIODO"
        ), dataset1 AS (
            SELECT C."FCN_GENERACION",
                   d."FCN_NUMERO_CUENTA" as "FCN_CUENTA",
                   C."FTC_AHORRO",
                   C."FTC_DES_CONCEPTO",
                   C."FTC_SECCION",
                   replace(PR."FTC_PERIODO", '/','') AS "PERIODO",
                   F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
                   sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_ABONO" ELSE 0 END) AS aportaciones,
                   sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_CARGO" ELSE 0 END) AS retiros,
                   sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_COMISION" ELSE 0 END) AS comisiones,
                   sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND R."FCN_ID_PERIODO" = (SELECT I."FTN_ID_PERIODO" FROM SaldoIni I) THEN R."FTF_SALDO_INICIAL" ELSE 0 END) AS saldoInicial,
                   sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") AND R."FCN_ID_PERIODO" = (SELECT I."FTN_ID_PERIODO" FROM SaldoFin I) THEN R."FTF_SALDO_FINAL" ELSE 0 END) AS saldoFinal
            FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_GENERACION" = P."FTN_ID_PERIODICIDAD"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" PG ON F."FCN_ID_PERIODICIDAD_ANVERSO" = PG."FTN_ID_PERIODICIDAD"
                INNER JOIN dataset D ON D."FTN_ID_FORMATO" = F."FCN_ID_FORMATO_ESTADO_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_CONFIGURACION_ANVERSO" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
                INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON D."FCN_NUMERO_CUENTA" = R."FCN_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
                -- INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" MU on MU."FCN_CUENTA" = R."FCN_CUENTA"
                INNER JOIN "GESTOR"."TCGESPRO_PERIODO" PR ON PR."FTN_ID_PERIODO" = :term
            GROUP BY D."FCN_NUMERO_CUENTA",
                     C."FTC_AHORRO",
                     C."FTC_DES_CONCEPTO",
                     C."FTC_SECCION",
                     F."FCN_ID_FORMATO_ESTADO_CUENTA",
                     PR."FTC_PERIODO",
                     C."FCN_GENERACION"
            LIMIT 100000
        )
        SELECT "FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
               -- "PERIODO",
               -- "FTN_ID_FORMATO",
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
               "FTC_SECCION" AS "FTC_SECCION",
               "FTC_AHORRO" AS "FTC_TIPO_AHORRO",
               --now() AS "FTD_FECHAHORA_ALTA",
               :user AS FTC_USUARIO_ALTA--,
               -- FTD_FECHA_INICIO",
               -- "FTD_FECHA_FINAL"
        FROM dataset1
        """, {"term": term_id, "start": start_month, "end": end_month, "user": str(user)})

        # anverso_df = anverso_df.drop(col("PERIODO"))
        # anverso_df = anverso_df.drop(col("FTN_ID_FORMATO"))
        anverso_df.createOrReplaceTempView("anverso")

        general_df = spark.sql("""
        SELECT C.*, ASS.FTN_MONTO AS FTN_SALDO_SUBTOTAL, AST.FTN_MONTO AS FTN_SALDO_TOTAL
        FROM general C
            INNER JOIN (
                SELECT FCN_NUMERO_CUENTA, SUM(FTN_SALDO_FINAL) AS FTN_MONTO
                FROM anverso
                WHERE FTC_SECCION = 'AHO'
                GROUP BY FCN_NUMERO_CUENTA
            ) ASS ON C.FCN_NUMERO_CUENTA = ASS.FCN_NUMERO_CUENTA
            INNER JOIN (
                SELECT FCN_NUMERO_CUENTA, SUM(FTN_SALDO_FINAL) AS FTN_MONTO
                FROM anverso
                GROUP BY FCN_NUMERO_CUENTA
            ) AST ON C.FCN_NUMERO_CUENTA = AST.FCN_NUMERO_CUENTA
        -- WHERE C.FCN_ID_EDOCTA IS NOT NULL
        """)

        reverso_df = spark.sql("""
        SELECT R.*, G.FCN_ID_EDOCTA
        FROM reverso R
            INNER JOIN general G ON G.FCN_NUMERO_CUENTA = R.FCN_NUMERO_CUENTA
        WHERE R.FCN_NUMERO_CUENTA IN (SELECT FCN_NUMERO_CUENTA FROM anverso)
        """)

        anverso_df = spark.sql("""
        SELECT A.*, G.FCN_ID_EDOCTA 
        FROM anverso A
            INNER JOIN general G ON G.FCN_NUMERO_CUENTA = A.FCN_NUMERO_CUENTA
        """)

        _write_spark_dataframe(reverso_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_REVERSO')
        _write_spark_dataframe(anverso_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_ANVERSO')
        _write_spark_dataframe(general_df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_GENERAL')
        # _write_spark_dataframe(df, configure_postgres_sp

        notify(
            postgres,
            "Generacion finales",
            phase,
            area,
            term=term_id,
            message="Se terminaron de generar los estados de cuenta finales con éxito",
            aprobar=False,
            descarga=False,
        )
