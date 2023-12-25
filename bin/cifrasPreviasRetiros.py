from profuturo.common import register_time, define_extraction, notify, truncate_table
from profuturo.database import get_postgres_pool, get_buc_pool, configure_postgres_spark, configure_mit_spark
from profuturo.extraction import _get_spark_session, _write_spark_dataframe, read_table_insert_temp_view
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
import sys
from datetime import datetime

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start = term["start_month"]
    end = term["end_month"]
    end_month_anterior = term["end_saldos_anterior"]
    valor_accion_anterior = term["valor_accion_anterior"]
    print(end_month_anterior, valor_accion_anterior)
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
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

        read_table_insert_temp_view(configure_postgres_spark, query, "retiros", params={"term": term_id, "user": str(user), "area": area})
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

