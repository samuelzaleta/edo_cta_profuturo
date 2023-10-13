from profuturo.common import notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, configure_postgres_spark
from profuturo.extraction import extract_terms, _get_spark_session, _write_spark_dataframe
from profuturo.reporters import HtmlReporter
import pyspark.sql.functions as f
from datetime import datetime
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session()

    with register_time(postgres_pool, phase, term_id, user, area):
        movements = ['MIT', 'INTEGRITY']

        #Getting data from DB
        for movement in movements:
            query = f"""
                            select distinct msrc.*
                            from "GESTOR"."TCGESPRO_MUESTRA_SOL_RE_CONSAR" msrc
                            left join "GESTOR"."TCGESPRO_MUESTRA" m
                                on m."FTN_ID_MUESTRA" = msrc."FCN_ID_MUESTRA"
                            left join "GESTOR"."TCGESPRO_PERIODO_AREA" pa
                                on pa."FCN_ID_PERIODO" = m."FCN_ID_PERIODO" 
                            left join "GESTOR"."TTGESPRO_MOV_PROFUTURO_CONSAR" mpc 
                                on msrc."FCN_ID_MOVIMIENTO_CONSAR" = mpc."FCN_ID_MOVIMIENTO_CONSAR"
                            left join "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" mp
                                on mpc."FCN_ID_MOVIMIENTO_PROFUTURO" = mp."FTN_ID_MOVIMIENTO_PROFUTURO" 
                            where pa."FCN_ID_AREA" = 1
                                and pa."FTB_ESTATUS" = true
                                and mp."FTC_ORIGEN" = '{movement}'
                                and msrc."FTC_STATUS" = 'Reproceso'
                            """
            if movement == "INTEGRITY":
                query = query + """\nand mp."FTB_SWITCH" = true"""

            df = spark.sql(query)

            id_consar_movements = (data[0] for data in df.select("FCN_ID_MOVIMIENTO_CONSAR").collect())

            #Eliminating records from the reprocesing table
            delete_records = f"""
            delete from "GESTOR"."TCGESPRO_MUESTRA_SOL_RE_CONSAR"
            where "FCN_ID_MOVIMIENTO_CONSAR" in {id_consar_movements}
            """

            update_df = df.withColumn("FTC_STATUS", f.lit("REPROCESADO"))

            #Upload updating data
            _write_spark_dataframe(update_df, configure_postgres_spark, '"GESTOR"."TCGESPRO_MUESTRA_SOL_RE_CONSAR"')

            # Cifras de control
            report = html_reporter.generate(
                postgres,
                query,
                ["Solicitud de Reproceso", "ID Muestra", "ID Movimiento CONSAR", "Estatus", "Comentario Solicitud", "Comentario Estatus"],
                [],
                params={"term": term_id},
            )

            notify(
                postgres,
                f"Cifras de control Movimientos {movement} - {datetime.now()}",
                phase,
                area,
                term=term_id,
                message=f"Se han generado las cifras de control para movimientos {movement} exitosamente para el periodo {time_period}",
                details=report,
            )
