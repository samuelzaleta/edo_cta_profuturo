from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local') \
    .appName("myspark") \
    .getOrCreate()

sql = """
SELECT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
       SH.FCN_ID_SIEFORE,
       SH.FCN_ID_TIPO_SUBCTA,
       SH.FTD_FEH_LIQUIDACION,
       'I' AS FTC_TIPO_SALDO,
       MAX(VA.FCD_FEH_ACCION) AS FCD_FEH_ACCION,
       SUM(SH.FTN_DIA_ACCIONES) AS FTN_DIA_ACCIONES,
       SUM(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION) AS FTF_SALDO_DIA
FROM cierren.thafogral_saldo_historico_v2 SH
INNER JOIN TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
INNER JOIN (
    SELECT SHMAX.FTN_NUM_CTA_INVDUAL,
           SHMAX.FCN_ID_SIEFORE,
           SHMAX.FCN_ID_TIPO_SUBCTA,
           MAX(TRUNC(SHMAX.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
    FROM cierren.thafogral_saldo_historico_v2 SHMAX
    WHERE SHMAX.FTD_FEH_LIQUIDACION <= date '2023-01-31'
      -- AND SHMAX.FTN_NUM_CTA_INVDUAL = 10044531
      -- AND SHMAX.FCN_ID_TIPO_SUBCTA = 22
      -- AND SHMAX.FCN_ID_SIEFORE = 83
    GROUP BY SHMAX.FTN_NUM_CTA_INVDUAL, SHMAX.FCN_ID_SIEFORE, SHMAX.FCN_ID_TIPO_SUBCTA
) SHMAXIMO ON SH.FTN_NUM_CTA_INVDUAL = SHMAXIMO.FTN_NUM_CTA_INVDUAL
          AND SH.FCN_ID_TIPO_SUBCTA = SHMAXIMO.FCN_ID_TIPO_SUBCTA AND SH.FCN_ID_SIEFORE = SHMAXIMO.FCN_ID_SIEFORE
          AND SH.FTD_FEH_LIQUIDACION = SHMAXIMO.FTD_FEH_LIQUIDACION
INNER JOIN (
    SELECT ROW_NUMBER() OVER(PARTITION BY FCN_ID_SIEFORE, FCN_ID_REGIMEN ORDER BY FCD_FEH_ACCION DESC) AS ROW_NUM,
           FCN_ID_SIEFORE, FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
    FROM TCAFOGRAL_VALOR_ACCION
    WHERE FCD_FEH_ACCION <= date '2023-01-31'
) VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
    AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
    AND VA.ROW_NUM = 1
GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, SH.FTD_FEH_LIQUIDACION
"""

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//172.22.164.17:1521/mitafore.profuturo-gnp.net") \
    .option('driver', 'oracle.jdbc.driver.OracleDriver') \
    .option("oracle.jdbc.timezoneAsRegion", "false") \
    .option("dbtable", f"({sql}) test_test") \
    .option("user", "CIERREN_APP") \
    .option("password", "l90E5$TT") \
    .load()

print("Extracting...")
df.show()
print("Done show! Counting...")
print(df.count())
print("Done counting!")
