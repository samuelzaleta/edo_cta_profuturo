from profuturo.common import notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import extract_terms, _get_spark_session, _write_spark_dataframe, extract_dataset_spark
from profuturo.reporters import HtmlReporter
import pyspark.sql.functions as f
from datetime import datetime
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])
table = '"HECHOS"."TTHECHOS_MOVIMIENTO"'

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):

        #Getting data from DB
        query_reproceso = f"""
            select distinct msrc.*
            from "GESTOR"."TCGESPRO_MUESTRA_SOL_RE_CONSAR" msrc
            inner join "GESTOR"."TCGESPRO_MUESTRA" m
                on m."FTN_ID_MUESTRA" = msrc."FCN_ID_MUESTRA"
            inner join "GESTOR"."TCGESPRO_PERIODO_AREA" pa
                on pa."FCN_ID_PERIODO" = m."FCN_ID_PERIODO"
            inner join "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" mpc
                on msrc."FCN_ID_MOVIMIENTO_CONSAR" = mpc."FCN_ID_MOVIMIENTO_CONSAR"
            inner join "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" mp
                on mpc."FCN_ID_MOVIMIENTO_PROFUTURO" = mp."FTN_ID_MOVIMIENTO_PROFUTURO" 
            where pa."FCN_ID_AREA" = {area}
                and pa."FTB_ESTATUS" = true
                and msrc."FTC_STATUS" = 'Aprobado'
                and mp."FTB_SWITCH" = true;
            """

        query_muestra = f"""
            select distinct m.*
            from "GESTOR"."TCGESPRO_MUESTRA_SOL_RE_CONSAR" msrc
            inner join "GESTOR"."TCGESPRO_MUESTRA" m
                on m."FTN_ID_MUESTRA" = msrc."FCN_ID_MUESTRA"
            inner join "GESTOR"."TCGESPRO_PERIODO_AREA" pa
                on pa."FCN_ID_PERIODO" = m."FCN_ID_PERIODO"
            inner join "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" mpc
                on msrc."FCN_ID_MOVIMIENTO_CONSAR" = mpc."FCN_ID_MOVIMIENTO_CONSAR"
            inner join "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" mp
                on mpc."FCN_ID_MOVIMIENTO_PROFUTURO" = mp."FTN_ID_MOVIMIENTO_PROFUTURO" 
            where pa."FCN_ID_AREA" = {area}
                and pa."FTB_ESTATUS" = true
                and msrc."FTC_STATUS" = 'Aprobado'
                and mp."FTB_SWITCH" = true;
            """


        df_reproceso = spark.sql(query_reproceso)
        df_muestra = spark.sql(query_muestra)

        id_consar_movements = (data[0] for data in df_reproceso.select("FCN_ID_MOVIMIENTO_CONSAR").collect())
        id_muestra = (data[0] for data in df_muestra.select("FTN_ID_MUESTRA").collect())

        #Deleting records
        spark.sql(f"""
                delete from "GESTOR"."TCGESPRO_MUESTRA_SOL_RE_CONSAR"
                where "FCN_ID_MOVIMIENTO_CONSAR" in {id_consar_movements}
                """)
        spark.sql(f"""
                delete from "GESTOR"."TCGESPRO_MUESTRA"
                where "FTN_ID_MUESTRA" in {id_muestra}
                and "FCN_ID_PERIODO" = {term}
                """)
        spark.sql(f"""
                delete from "HECHOS"."TTHECHOS_MOVIMIENTO"
                where "FCN_ID_CONCEPTO_MOVIMIENTO" in {id_consar_movements}
                and "FCN_ID_PERIODO" = {term}
                """)


        update_df_reproceso = df_reproceso.withColumn("FTC_STATUS", f.lit("Reprocesado"))
        update_df_muestra = df_muestra.withColumn("FTN_ID_MUESTRA", f.lit("Reprocesado"))

        #Upload updating data
        _write_spark_dataframe(update_df_reproceso, configure_postgres_spark, '"GESTOR"."TCGESPRO_MUESTRA_SOL_RE_CONSAR"')
        _write_spark_dataframe(update_df_muestra, configure_postgres_spark,'"GESTOR"."TCGESPRO_MUESTRA"')
        mov_tables_ref = ["TTAFOGRAL_MOV_AVOL", "TTAFOGRAL_MOV_RCV", "TTAFOGRAL_MOV_COMP"]
        mov_tables = ["TTAFOGRAL_MOV_BONO", "TTAFOGRAL_MOV_GOB", "TTAFOGRAL_MOV_SAR", "TTAFOGRAL_MOV_VIV"]

        for mov_table_ref in mov_tables_ref:
            query_mov_ref = f"""
                    SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
                           FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
                           FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
                           FCN_ID_TIPO_SUBCTA,
                           FCN_ID_SIEFORE,
                           FTC_FOLIO,
                           FNN_ID_REFERENCIA AS FTN_REFERENCIA,
                           FTF_MONTO_ACCIONES,
                           ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
                           FTD_FEH_LIQUIDACION,
                           'M' AS FTC_BD_ORIGEN
                    FROM {mov_table_ref}
                    WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
                    """
            extract_dataset_spark(configure_mit_spark, configure_postgres_spark, query_mov_ref, table, term=term_id, params={"start": start_month, "end": end_month})

        for mov_table in mov_tables:
            query_mov = f"""
                SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
                       FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
                       FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
                       FCN_ID_TIPO_SUBCTA,
                       FCN_ID_SIEFORE,
                       FTC_FOLIO,
                       FTF_MONTO_ACCIONES,
                       ROUND(FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
                       FTD_FEH_LIQUIDACION,
                       'M' AS FTC_BD_ORIGEN
                FROM {mov_table}
                WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
                """

            extract_dataset_spark(configure_mit_spark, configure_postgres_spark, query_mov, table, term=term_id, params={"start": start_month, "end": end_month})

        # Cifras de control
        report = html_reporter.generate(
            postgres,
            query_reproceso,
            ["Solicitud de Reproceso", "ID Muestra", "ID Movimiento CONSAR", "Estatus", "Comentario Solicitud", "Comentario Estatus"],
            [],
            params={"term": term_id},
        )

        notify(
            postgres,
            f"Cifras de control de reprocesamiento consar",
            phase,
            area,
            term=term_id,
            message=f"Se han generado las cifras de control para reprocesamiento consar exitosamente para el periodo {time_period}",
            details=report,
        )
