from sqlalchemy import text
from profuturo.common import define_extraction, register_time
from profuturo.database import get_postgres_pool, configure_postgres_spark
from profuturo.extraction import extract_terms, extract_dataset_spark
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

    with register_time(postgres_pool, phase=phase, area=area, usuario=user, term=term_id):
        anverso = postgres.execute(text("""
        SELECT I."FCN_CUENTA",
               C."FTC_CONCEPTO",
               sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_ABONO" ELSE 0 END) AS aportaciones,
               sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_CARGO" ELSE 0 END) AS retiros,
               sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_RENDIMIENTO_CALCULADO" ELSE 0 END) AS rendimientos,
               sum(CASE WHEN R."FCN_ID_TIPO_SUBCTA" = ANY(C."FTA_SUBCUENTAS") THEN R."FTF_COMISION" ELSE 0 END) AS comisiones
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
            INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON T."FTN_ID_PERIODO" = 27
            INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
            INNER JOIN "GESTOR"."TEMP_CONFIGURACION" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
            INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON T."FTN_ID_PERIODO" = I."FCN_ID_PERIODO" AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
            INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA" AND I."FCN_ID_PERIODO" = R."FCN_ID_PERIODO"
        WHERE mod(extract(MONTH FROM to_date(T."FTC_PERIODO", 'MM/YYYY')), P."FTN_MESES") = 0
        --  AND now() BETWEEN to_date(CF."FTD_INICIO_VIGENCIA", 'MM/YYYY') AND coalesce(date_trunc(to_date("FTD_FIN_VIGENCIA", 'MM/YYYY')), now())
        GROUP BY I."FCN_CUENTA", C."FTC_CONCEPTO";
        """)).all()

        reverso = postgres.execute(text("""
        select
        R."FCN_CUENTA",
        PC."FTC_DESCRIPCION",
        sum(R."FTF_MONTO_PESOS")
        FROM "GESTOR"."TTGESPRO_CONFIGURACION_FORMATO_ESTADO_CUENTA" F
        INNER JOIN "GESTOR"."TCGESPRO_PERIODO" T ON T."FTN_ID_PERIODO" = 1
        INNER JOIN "GESTOR"."TCGESPRO_PERIODICIDAD" P ON F."FCN_ID_PERIODICIDAD_ANVERSO" = P."FTN_ID_PERIODICIDAD"
        INNER JOIN "GESTOR"."TEMP_CONFIGURACION" C ON F."FCN_ID_GENERACION" = C."FCN_GENERACION"
        INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON T."FTN_ID_PERIODO" = I."FCN_ID_PERIODO" AND F."FCN_ID_GENERACION" = CASE I."FTC_GENERACION" WHEN 'AFORE' THEN 2 WHEN 'TRANSICION' THEN 3 END
        INNER JOIN "HECHOS"."TTHECHOS_MOVIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA" --AND I."FCN_ID_PERIODO" = R."FCN_ID_PERIODO"
        INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" PC ON R."FCN_ID_CONCEPTO_MOVIMIENTO" = PC."FTN_ID_MOVIMIENTO_PROFUTURO"
        --INNER JOIN "HECHOS"."TTCALCUL_RENDIMIENTO" R ON I."FCN_CUENTA" = R."FCN_CUENTA" AND I."FCN_ID_PERIODO" = R."FCN_ID_PERIODO"
        WHERE mod(extract(MONTH FROM to_date(T."FTC_PERIODO", 'MM/YYYY')), P."FTN_MESES") = 0
        GROUP BY
        R."FCN_CUENTA",
        PC."FTC_DESCRIPCION";
        """))
