from profuturo.common import define_extraction, register_time
from profuturo.database import get_postgres_pool, configure_postgres_spark, configure_bigquery_spark
from profuturo.extraction import _write_spark_dataframe, extract_terms, extract_dataset_spark, _get_spark_session, read_table_insert_temp_view
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import udf, concat, col, current_date
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

        read_table_insert_temp_view(configure_postgres_spark,
        f"""
           SELECT
           DISTINCT
           --M."FTC_URL_PDF_ORIGEN" as "FTC_URL_EDOCTA",
           F."FCN_ID_GENERACION" AS "FTN_ID_GRUPO_SEGMENTACION",
           'CANDADO' AS "FTC_CANDADO_APERTURA",
           F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
           M."FCN_ID_PERIODO",
           REPLACE(PR."FTC_PERIODO", '/','') AS "PERIODO",
           C."FTN_CUENTA" AS "FCN_NUMERO_CUENTA",
           -- as "FTB_PDF_IMPRESO",
           --current_date AS "FTD_FECHA_CORTE",
           --current_date AS "FTD_FECHA_GRAL_INICIO",
           --current_date AS "FTD_FECHA_GRAL_FIN",
           --current_date AS "FTD_FECHA_MOV_INICIO",
           --current_date AS "FTD_FECHA_MOV_FIN",
           0.0 AS "FTN_SALDO_SUBTOTAL",
           0.0 AS "FTN_SALDO_TOTAL",
           0 AS "FTN_ID_SIEFORE",
           Cast(I."FTC_PERFIL_INVERSION" as varchar) AS "FTC_DESC_SIEFORE",
           Cast(I."FTC_GENERACION" as varchar) AS "FTC_DESC_TIPOGENERACION",
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
           INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P
           ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
           INNER JOIN "GESTOR"."TEMP_CONFIGURACION" CF
           ON F."FCN_ID_GENERACION" = CF."FCN_GENERACION"
           INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I
           ON I."FCN_ID_PERIODO" = 1 --:term
           AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
           INNER JOIN "MAESTROS"."TCDATMAE_CLIENTE" C
           ON I."FCN_CUENTA" = C."FTN_CUENTA"
           INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" M
           ON C."FTN_CUENTA" = M."FCN_CUENTA"
           INNER JOIN "GESTOR"."TCGESPRO_PERIODO" PR
           ON PR."FTN_ID_PERIODO" = 1 --:term
       """, "edoCtaGenerales",
        params={"term": term_id, "user": str(user)}
        )

        uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())
        df = spark.sql("""
                                   select  * from edoCtaGenerales
                                   """)

        df = df.withColumn("FCN_ID_EDOCTA", concat(
            col("FCN_NUMERO_CUENTA"),
            col("PERIODO"),
            col("FTN_ID_FORMATO"),
        ).cast("bigint"))
        df.select("FCN_ID_EDOCTA").show()

        df = df.withColumn("FCN_FOLIO", uuidUdf())
        df = df.drop(col("PERIODO"))

        _write_spark_dataframe(df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_GENERAL')
        df.createOrReplaceTempView("general")
        read_table_insert_temp_view(
            configure_postgres_spark,
            f"""
                      select 
                      --F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FCN_ID_EDOCTA",
                             REPLACE(PR."FTC_PERIODO", '/','') AS "PERIODO",
                             F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
                             MC."FTC_MOV_TIPO_AHORRO" AS "FTC_SECCION",
                             R."FCN_CUENTA" AS "FCN_NUMERO_CUENTA",
                             R."FTD_FEH_LIQUIDACION" as "FTD_FECHA_MOVIMIENTO",
                             MC."FTN_ID_MOVIMIENTO_CONSAR" AS "FTN_ID_CONCEPTO",
                             MC."FTC_DESCRIPCION" AS "FTC_DESC_CONCEPTO",
                             'Desconocido' AS "FTC_PERIODO_REFERENCIA",
                             sum(R."FTF_MONTO_PESOS") as "FTN_MONTO",
                             0 as "FTN_DIA_COTIZADO",
                             cast(0.0 as numeric) as "FTN_SALARIO_BASE", 
                             now() AS "FTD_FECHAHORA_ALTA",
                             cast(:user as varchar) AS "FTC_USUARIO_ALTA"
                      FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                          INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
                          INNER JOIN "GESTOR"."TEMP_CONFIGURACION" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
                          INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON I."FCN_ID_PERIODO" = :term AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
                          INNER JOIN "HECHOS"."TTHECHOS_MOVIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA"
                          INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
                          INNER JOIN "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON R."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
                          INNER JOIN "MAESTROS"."TCDATMAE_MOVIMIENTO_CONSAR" MC ON PC."FCN_ID_MOVIMIENTO_CONSAR" = MC."FTN_ID_MOVIMIENTO_CONSAR"
                          INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" MU on MU."FCN_CUENTA" = R."FCN_CUENTA"
                          INNER JOIN "GESTOR"."TCGESPRO_PERIODO" PR
                          ON PR."FTN_ID_PERIODO" = 1 --:term 
                      --INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" PC ON R."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FTN_ID_MOVIMIENTO_PROFUTURO"
                      --INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA" AND I."FCN_ID_PERIODO" = R."FCN_ID_PERIODO"
                      WHERE mod(extract(MONTH FROM to_date(T."FTC_PERIODO", 'MM/YYYY')), P."FTN_MESES") = 0
                        AND to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * P."FTN_MESES" AND :end
                      GROUP BY  F."FCN_ID_FORMATO_ESTADO_CUENTA", PR."FTC_PERIODO", F."FCN_ID_FORMATO_ESTADO_CUENTA", R."FCN_CUENTA", MC."FTN_ID_MOVIMIENTO_CONSAR", R."FTD_FEH_LIQUIDACION"
                      """,
            "reverso",
            params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)},
        )
        uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())
        df = spark.sql("""
                                              select  * from reverso
                                              """)
        df = df.withColumn("FCN_ID_EDOCTA", concat(
            col("FCN_NUMERO_CUENTA"),
            col("PERIODO"),
            col("FTN_ID_FORMATO"),
        ).cast("bigint"))

        # df = df.withColumn("FCN_FOLIO", uuidUdf())
        df = df.drop(col("PERIODO"))
        df = df.drop(col("FTN_ID_FORMATO"))
        _write_spark_dataframe(df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_REVERSO')

        read_table_insert_temp_view(
            configure_postgres_spark,
            f"""WITH SaldoIni as (
                    select
                    T."FTN_ID_PERIODO",
                    min(to_date(T."FTC_PERIODO", 'MM/YYYY'))
                    FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                    INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
                    INNER JOIN "GESTOR"."TEMP_CONFIGURACION" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
                    INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON :term = I."FCN_ID_PERIODO" AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
                    INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA"
                    INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
                    INNER JOIN "GESTOR"."TCGESPRO_MUESTRA" MU on MU."FCN_CUENTA" = R."FCN_CUENTA"
                    WHERE to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * P."FTN_MESES" AND :end
                    GROUP BY 1
                    )
                    ,SaldoFin as (
                    select
                    T."FTN_ID_PERIODO",
                    max(to_date(T."FTC_PERIODO", 'MM/YYYY'))
                    FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                    INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
                    INNER JOIN "GESTOR"."TEMP_CONFIGURACION" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
                    INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON :term = I."FCN_ID_PERIODO" AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
                    INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA"
                    INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
                    inner join "GESTOR"."TCGESPRO_MUESTRA" MU on MU."FCN_CUENTA" = R."FCN_CUENTA"
                    WHERE to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * P."FTN_MESES" AND :end
                    GROUP BY 1
                    ),
                    dataset AS (
                    select
                    C."FCN_GENERACION",
                    I."FCN_CUENTA",
                    C."FTC_AHORRO",
                    C."FTC_DES_CONCEPTO",
                    C."FTC_SECCION",
                    REPLACE(PR."FTC_PERIODO", '/','') AS "PERIODO",
                    F."FCN_ID_FORMATO_ESTADO_CUENTA" AS "FTN_ID_FORMATO",
                    sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_ABONO" ELSE 0 END) AS aportaciones,
                    sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_CARGO" ELSE 0 END) AS retiros,
                    --sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_RENDIMIENTO_CALCULADO" ELSE 0 END) AS rendimientos,
                    sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_COMISION" ELSE 0 END) AS comisiones,
                    sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" =  ANY(C."FTA_SUBCUENTAS") AND R."FCN_ID_PERIODO" = (Select I."FTN_ID_PERIODO" from SaldoIni I) THEN R."FTF_SALDO_INICIAL" ELSE 0 END) AS saldoInicial,
                    sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" =  ANY(C."FTA_SUBCUENTAS") AND R."FCN_ID_PERIODO" = (Select I."FTN_ID_PERIODO" from SaldoFin I) THEN R."FTF_SALDO_FINAL" ELSE 0 END) AS saldoFinal
                    FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
                    INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
                    INNER JOIN "GESTOR"."TEMP_CONFIGURACION" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
                    INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON :term = I."FCN_ID_PERIODO" AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
                    INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA"
                    INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON R."FCN_ID_PERIODO" = T."FTN_ID_PERIODO"
                    inner join "GESTOR"."TCGESPRO_MUESTRA" MU on MU."FCN_CUENTA" = R."FCN_CUENTA"
                    INNER JOIN "GESTOR"."TCGESPRO_PERIODO" PR
                    ON PR."FTN_ID_PERIODO" = 1 --:term
                    WHERE mod(extract(MONTH FROM :end), P."FTN_MESES") = 0
                    AND to_date(T."FTC_PERIODO", 'MM/YYYY') BETWEEN :start - INTERVAL '1 month' * P."FTN_MESES" AND :end
                    --  AND CURRENT_DATE() BETWEEN to_date(CF."FTD_INICIO_VIGENCIA", 'MM/YYYY') AND coalesce(date_trunc(to_date("FTD_FIN_VIGENCIA", 'MM/YYYY')), now())
                    GROUP BY
                    I."FCN_CUENTA",
                    C."FTC_AHORRO",
                    C."FTC_DES_CONCEPTO",
                    C."FTC_SECCION",
                    F."FCN_ID_FORMATO_ESTADO_CUENTA",
                    PR."FTC_PERIODO",
                    C."FCN_GENERACION"
                    )
                    SELECT
                    "FCN_CUENTA" as "FCN_NUMERO_CUENTA",
                    "PERIODO",
                    "FTN_ID_FORMATO",
                    "FTC_DES_CONCEPTO" as "FTC_CONCEPTO_NEGOCIO",
                    cast(aportaciones as numeric(16,2)) as "FTF_APORTACION",
                    cast(retiros as numeric(16,2)) as "FTN_RETIRO",
                    cast(comisiones as numeric(16,2)) as "FTN_COMISION",
                    cast(saldoInicial as numeric(16,2)) as "FTN_SALDO_ANTERIOR",
                    cast(saldoFinal as numeric(16,2))  as "FTN_SALDO_FINAL",
                    Cast((saldoFinal - (aportaciones + saldoInicial - comisiones - retiros)) as numeric(16,2)) AS "FTN_RENDIMIENTO",
                    cast(0.0 as numeric(16,2)) AS "FTN_VALOR_ACTUAL_PESO",
                    cast(0.000 as numeric(16,6)) AS "FTN_VALOR_ACTUAL_UDI",
                    cast(0.0 as numeric(16,2))  AS "FTN_VALOR_NOMINAL_PESO",
                    cast(0.000 as numeric(16,6)) AS "FTN_VALOR_NOMINAL_UDI",
                    1 AS "FTN_TIPO_PENSION",
                    cast(0.0 as numeric(16,2)) AS "FTN_MONTO_PENSION",
                    "FTC_SECCION" AS  "FTC_SECCION",
                    "FTC_AHORRO" AS "FTC_TIPO_AHORRO",
                    now() AS "FTD_FECHAHORA_ALTA",
                    '0' AS "FTC_USUARIO_ALTA"
                    FROM dataset
                       """,
            "anverso",
            params={"term": term_id, "start": start_month, "end": end_month, "user": str(user)},
        )

        uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())
        df = spark.sql("""
                                       select  * from anverso
                                       """)
        df = df.withColumn("FCN_ID_EDOCTA", concat(
            col("FCN_NUMERO_CUENTA"),
            col("PERIODO"),
            col("FTN_ID_FORMATO"),
        ).cast("bigint"))

        # df = df.withColumn("FCN_FOLIO", uuidUdf())
        df = df.drop(col("PERIODO"))
        df = df.drop(col("FTN_ID_FORMATO"))
        _write_spark_dataframe(df, configure_bigquery_spark, 'ESTADO_CUENTA.TTMUESTR_ANVERSO')

        read_table_insert_temp_view(configure_postgres_spark,
        f"""
                SELECT
                "FTN_ID_MUESTRA",
                "FCN_CUENTA",
                "FCN_ID_PERIODO",
                "FTC_URL_PDF_ORIGEN",
                "FTC_ESTATUS",
                "FCN_ID_USUARIO",
                "FCN_ID_AREA",
                "FTD_FECHAHORA_ALTA"
                FROM "GESTOR"."TCGESPRO_MUESTRA"
              """, "muestras",
              params={"term": term_id, "user": str(user)}
            )
        df = spark.sql("""
        select
        "FTN_ID_MUESTRA",
        "FCN_CUENTA",
        "FCN_ID_PERIODO",
        Concat("https://storage.cloud.google.com/profuturo-archivos/",e.FCN_FOLIO) as "FTC_URL_PDF_ORIGEN",
        "FTC_ESTATUS",
        "FCN_ID_USUARIO",
        "FCN_ID_AREA",
        "FTD_FECHAHORA_ALTA"
        from muestras m
        right join general e
        on m.FCN_CUENTA = e.FCN_NUMERO_CUENTA  
        """)

        df.show(1)






