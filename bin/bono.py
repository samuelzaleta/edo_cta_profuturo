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

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    end_month_anterior = term["end_saldos_anterior"]
    valor_accion_anterior = term["valor_accion_anterior"]
    print(end_month_anterior, valor_accion_anterior)
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        truncate_table(postgres, 'TTHECHOS_RETIRO', term=term_id)
        #truncate_table(postgres, 'TTHECHOS_RETIRO_LIQUIDACIONES', term=term_id)
        #truncate_table(postgres, 'TTHECHOS_RETIRO_SALDOS_INICIALES', term=term_id)

        query_cuentas_bono = """
        SELECT
        DISTINCT X.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
        FCC_VALOR_IND AS CUENTA_VIGENTE
        FROM(
        SELECT DISTINCT FTN_NUM_CTA_INVDUAL
        FROM CIERREN.TTAFOGRAL_MOV_BONO
        WHERE FTD_FEH_LIQUIDACION  between add_months(DATE '2023-01-31', -4) and DATE '2023-01-31' --:start AND :end
        AND FCN_ID_TIPO_SUBCTA = 14
    
        UNION ALL
    
        SELECT DISTINCT  SHMAX.FTN_NUM_CTA_INVDUAL
        FROM cierren.thafogral_saldo_historico_v2 SHMAX
        WHERE SHMAX.FCN_ID_TIPO_SUBCTA = 14
              AND SHMAX.FTD_FEH_LIQUIDACION <= (
                  SELECT MIN(SHMIN.FTD_FEH_LIQUIDACION)
                  FROM cierren.thafogral_saldo_historico_v2 SHMIN
                  WHERE SHMIN.FTD_FEH_LIQUIDACION > DATE '2023-01-31' --start
                )
        ) x
        INNER JOIN CIERREN.TTAFOGRAL_IND_CTA_INDV IND
        ON X.FTN_NUM_CTA_INVDUAL = IND.FTN_NUM_CTA_INVDUAL
        AND FFN_ID_CONFIG_INDI = 2 AND  FTC_VIGENCIA= 1        
        """

        query_dias_rend_bono = """
         SELECT RB.FTN_NUM_CTA_INVDUAL, RB.FTD_FEH_VALOR AS FTD_FECHA_REDENCION_BONO,
                CAST(EXTRACT(DAY FROM RB.FTD_FEH_VALOR - TRUNC(CURRENT_DATE)) AS INTEGER) AS DIAS_PLAZO_NATURALES,
                COUNT(1) AS DIAS_INHABILIES,
                CAST(EXTRACT(DAY FROM RB.FTD_FEH_VALOR - TRUNC(CURRENT_DATE)) AS INTEGER)  - COUNT(1) DIA_PLAZO
        FROM TTAFOGRAL_OP_INVDUAL RB
        INNER JOIN TTAFOGRAL_CALENDARIO CAL
        ON CAL.FTD_FECHA BETWEEN CURRENT_DATE AND RB.FTD_FEH_VALOR AND FTN_DIA_HABIL = 0
        WHERE (RB.FTN_NUM_CTA_INVDUAL, RB.FTD_FEH_VALOR) IN (
                                        SELECT FTN_NUM_CTA_INVDUAL,
                                               MAX(FTD_FEH_VALOR)
                                        FROM TTAFOGRAL_OP_INVDUAL
                                        GROUP BY FTN_NUM_CTA_INVDUAL)
        GROUP BY RB.FTN_NUM_CTA_INVDUAL, RB.FTD_FEH_VALOR
        
        """

        query_vector = """
        SELECT FTN_PLAZO, FTN_FACTOR FROM TTAFOGRAL_VECTOR
        """

        query_valor_accion_bono = """
        SELECT FCN_VALOR_ACCION FROM TCAFOGRAL_VALOR_ACCION VA
        WHERE  VA.FCD_FEH_ACCION = DATE '2023-01-31' --:end
          AND FCN_ID_SIEFORE = 80
        """

        read_table_insert_temp_view(
            configure_mit_spark,
            query_cuentas_bono,
            "CUENTAS_BONO",
            params={"end": end_month}
        )

        read_table_insert_temp_view(
            configure_mit_spark,
            query_dias_rend_bono,
            "DIAS_RENDENCION",
            params={"end": end_month}
        )

        read_table_insert_temp_view(
            configure_mit_spark,
            query_vector,
            "VECTOR",
            params={"end": end_month}
        )

        read_table_insert_temp_view(
            configure_mit_spark,
            query_valor_accion_bono,
            "VALOR_ACCION",
            params={"end": end_month}
        )

