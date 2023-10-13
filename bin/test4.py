from profuturo.common import define_extraction, truncate_table
from profuturo.database import get_postgres_pool, get_buc_pool, configure_postgres_spark, configure_mit_spark
from profuturo.extraction import _get_spark_session, _write_spark_dataframe, read_table_insert_temp_view
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
buc_pool = get_buc_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool, buc_pool) as (postgres, buc):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    end_month_anterior = term["end_saldos_anterior"]
    valor_accion_anterior = term["valor_accion_anterior"]
    print(end_month_anterior,valor_accion_anterior, )
    spark = _get_spark_session()

    #print('phase',phase,'area', area, 'usuario',user, 'periodo', term_id)
    #with register_time(postgres_pool, phase, term_id, user, area):
    saldo_inicial_query_1 = f"""
            SELECT
                tsh."FCN_CUENTA"
                , tsh."FCN_ID_TIPO_SUBCTA" 
                , SUM(tsh."FTF_SALDO_DIA"::double precision) AS "FTF_SALDO_FINAL"
            FROM
                "HECHOS"."THHECHOS_SALDO_HISTORICO" tsh
            WHERE
                1=1
                AND tsh."FTC_TIPO_SALDO" = 'F'
                AND tsh."FTD_FEH_LIQUIDACION" BETWEEN :start_month AND :end_month_anterior
                --AND tsh."FCN_ID_PERIODO" = :term_id
            GROUP BY
                1=1
                , tsh."FCN_CUENTA"
                , tsh."FCN_ID_TIPO_SUBCTA"
            """

    saldo_inicial_query = f"""
    SELECT SH.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
           SH.FCN_ID_SIEFORE,
           SH.FCN_ID_TIPO_SUBCTA,
           SH.FTD_FEH_LIQUIDACION,
           :type AS FTC_TIPO_SALDO,
           MAX(VA.FCD_FEH_ACCION) AS FCD_FEH_ACCION,
           ROUND(SUM(SH.FTN_DIA_ACCIONES), 6) AS FTF_DIA_ACCIONES,
           ROUND(SUM(SH.FTN_DIA_ACCIONES * VA.FCN_VALOR_ACCION), 2) AS FTF_SALDO_INICIAL
    FROM cierren.thafogral_saldo_historico_v2 SH
    INNER JOIN cierren.TCCRXGRAL_TIPO_SUBCTA R ON R.FCN_ID_TIPO_SUBCTA = SH.FCN_ID_TIPO_SUBCTA
    INNER JOIN (
        SELECT SHMAX.FTN_NUM_CTA_INVDUAL,
               SHMAX.FCN_ID_SIEFORE,
               SHMAX.FCN_ID_TIPO_SUBCTA,
               MAX(TRUNC(SHMAX.FTD_FEH_LIQUIDACION)) AS FTD_FEH_LIQUIDACION
        FROM cierren.thafogral_saldo_historico_v2 SHMAX
        WHERE SHMAX.FTD_FEH_LIQUIDACION <= :date
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
        FROM cierren.TCAFOGRAL_VALOR_ACCION
        WHERE FCD_FEH_ACCION <= :accion
    ) VA ON SH.FCN_ID_SIEFORE = VA.FCN_ID_SIEFORE
        AND R.FCN_ID_REGIMEN = VA.FCN_ID_REGIMEN
        AND VA.ROW_NUM = 1
    GROUP BY SH.FTN_NUM_CTA_INVDUAL, SH.FCN_ID_SIEFORE, SH.FCN_ID_TIPO_SUBCTA, SH.FTD_FEH_LIQUIDACION
    """
    saldo_final_query = f"""
    SELECT
        tsh."FCN_CUENTA"
        , tsh."FCN_ID_TIPO_SUBCTA" 
        , SUM(tsh."FTF_SALDO_DIA"::double precision) AS "FTF_SALDO_FINAL"
    FROM
        "HECHOS"."THHECHOS_SALDO_HISTORICO" tsh
    WHERE
        1=1
        AND tsh."FTC_TIPO_SALDO" = 'F'
        AND tsh."FTD_FEH_LIQUIDACION" BETWEEN :start_month AND :end_month
        AND tsh."FCN_ID_PERIODO" = :term_id
    GROUP BY
        1=1
        , tsh."FCN_CUENTA"
        , tsh."FCN_ID_TIPO_SUBCTA"
    """
    cargo_query = f"""
    SELECT
        tmov."FCN_CUENTA"
        , tmov."FCN_ID_TIPO_SUBCTA"
        , SUM(tmov."FTF_MONTO_PESOS"::double precision) AS "FTF_CARGO"
    FROM
        "HECHOS"."TTHECHOS_MOVIMIENTO" tmov
    WHERE
        1=1
        AND tmov."FTD_FEH_LIQUIDACION" BETWEEN :start_month AND :end_month
        AND tmov."FCN_ID_TIPO_MOVIMIENTO" = '181'
        AND tmov."FCN_ID_PERIODO" = :term_id
    GROUP BY
        1=1
        , tmov."FCN_CUENTA"
        , tmov."FCN_ID_TIPO_SUBCTA"
    """
    abono_query = f"""
    SELECT
        tmov."FCN_CUENTA"
        , tmov."FCN_ID_TIPO_SUBCTA"
        , SUM(tmov."FTF_MONTO_PESOS"::double precision) AS "FTF_ABONO"
    FROM
        "HECHOS"."TTHECHOS_MOVIMIENTO" tmov
    WHERE
        1=1
        AND tmov."FTD_FEH_LIQUIDACION" BETWEEN :start_month AND :end_month
        AND tmov."FCN_ID_TIPO_MOVIMIENTO" = '180'
        AND tmov."FCN_ID_PERIODO" = :term_id
    GROUP BY
        1=1
        , tmov."FCN_CUENTA" 
        , tmov."FCN_ID_TIPO_SUBCTA"
    """
    comision_query = f"""
    SELECT
        C."FTN_NUM_CTA_INVDUAL" AS FCN_CUENTA,
        S."FCN_ID_TIPO_SUBCTA",
        SUM(C."FTF_MONTO_PESOS") AS "FTF_COMISION"
    FROM CIERREN.TTAFOGRAL_MOV_CMS C
    INNER JOIN CIERREN.TFCRXGRAL_CONFIG_MOV_ITGY M
    ON C.FCN_ID_CONCEPTO_MOV =M.FFN_ID_CONCEPTO_MOV
    INNER JOIN TRAFOGRAL_MOV_SUBCTA S ON M.FRN_ID_MOV_SUBCTA = S.FRN_ID_MOV_SUBCTA
    WHERE C."FTD_FEH_LIQUIDACION" BETWEEN :start_month AND :end_month
    GROUP BY C."FTN_NUM_CTA_INVDUAL", S."FCN_ID_TIPO_SUBCTA"
    """
    """
            read_table_insert_temp_view(
                configure_postgres_spark,
                saldo_inicial_query_1,
                "saldoinicial",
                params={"start_month ": start_month,
                        "end_month": end_month,
                        "term_id": term_id}
            )
    """
    truncate_table(postgres, "TTCALCUL_RENDIMIENTO", term=term_id)

    read_table_insert_temp_view(
        configure_mit_spark,
        saldo_inicial_query,
        "saldoinicial",
        params={"date": end_month_anterior, "type": "F", "accion": valor_accion_anterior}
    )

    read_table_insert_temp_view(
        configure_postgres_spark,
        saldo_final_query,
        "saldofinal",
        params={"start_month ": start_month,
                "end_month": end_month,
                "term_id": term_id}
    )
    read_table_insert_temp_view(
        configure_postgres_spark,
        cargo_query,
        "cargo",
        params={"start_month ": start_month,
                "end_month": end_month,
                "term_id": term_id}
    )
    read_table_insert_temp_view(
        configure_postgres_spark,
        abono_query,
        "abono",
        params={"start_month ": start_month,
                "end_month": end_month,
                "term_id": term_id}
    )
    read_table_insert_temp_view(
        configure_mit_spark,
        comision_query,
        "comision",
        params={"start_month ": start_month,
                "end_month": end_month}
    )

    df = spark.sql(f"""
        WITH tablon as (SELECT
            COALESCE(si.FCN_CUENTA, sf.FCN_CUENTA, ca.FCN_CUENTA, ab.FCN_CUENTA, cm.FCN_CUENTA) AS FCN_CUENTA
            , COALESCE(si.FCN_ID_TIPO_SUBCTA, sf.FCN_ID_TIPO_SUBCTA, ca.FCN_ID_TIPO_SUBCTA, ab.FCN_ID_TIPO_SUBCTA) AS FCN_ID_TIPO_SUBCTA
            , SUM(COALESCE(si.FTF_SALDO_INICIAL, 0)) AS FTF_SALDO_INICIAL
            , SUM(COALESCE(sf.FTF_SALDO_FINAL, 0)) AS FTF_SALDO_FINAL
            , SUM(COALESCE(ca.FTF_CARGO, 0)) AS FTF_CARGO
            , SUM(COALESCE(ab.FTF_ABONO, 0)) AS FTF_ABONO
            , SUM(COALESCE(cm.FTF_COMISION, 0)) AS FTF_COMISION
        FROM saldoinicial AS si
        LEFT JOIN abono AS ab
        ON si.FCN_CUENTA = ab.FCN_CUENTA
        AND si.FCN_ID_TIPO_SUBCTA = ab.FCN_ID_TIPO_SUBCTA
        LEFT JOIN comision AS cm
        ON si.FCN_CUENTA = cm.FCN_CUENTA
        AND si.FCN_ID_TIPO_SUBCTA = cm.FCN_ID_TIPO_SUBCTA
        LEFT JOIN cargo AS ca
        ON si.FCN_CUENTA = ca.FCN_CUENTA 
        AND si.FCN_ID_TIPO_SUBCTA = ca.FCN_ID_TIPO_SUBCTA
        FULL JOIN saldofinal AS sf
        ON si.FCN_CUENTA = sf.FCN_CUENTA 
        AND si.FCN_ID_TIPO_SUBCTA = ca.FCN_ID_TIPO_SUBCTA			
        GROUP BY 
        si.FCN_CUENTA, sf.FCN_CUENTA, ca.FCN_CUENTA, ab.FCN_CUENTA, cm.FCN_CUENTA,
        si.FCN_ID_TIPO_SUBCTA, sf.FCN_ID_TIPO_SUBCTA, ca.FCN_ID_TIPO_SUBCTA, ab.FCN_ID_TIPO_SUBCTA
        )
        select
        ta.FCN_CUENTA
        , {term_id} as FCN_ID_PERIODO
        , ta.FCN_ID_TIPO_SUBCTA
        , ta.FTF_SALDO_INICIAL
        , ta.FTF_SALDO_FINAL
        , ta.FTF_ABONO
        , ta.FTF_CARGO
        , ta.FTF_COMISION
        , (ta.FTF_SALDO_FINAL - (ta.FTF_ABONO + ta.FTF_SALDO_INICIAL - ta.FTF_COMISION - ta.FTF_CARGO)) AS FTF_RENDIMIENTO_CALCULADO
    FROM tablon AS ta
    WHERE (ta.FTF_SALDO_FINAL - (ta.FTF_ABONO + ta.FTF_SALDO_INICIAL - ta.FTF_COMISION - ta.FTF_CARGO)) > 0 
    """)
    df.show(5)
    _write_spark_dataframe(df, configure_postgres_spark, '"HECHOS"."TTCALCUL_RENDIMIENTO"')




