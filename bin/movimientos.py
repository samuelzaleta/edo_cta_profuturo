from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, configure_mit_spark, configure_postgres_spark
from profuturo.extraction import extract_terms, extract_dataset_spark
from profuturo.reporters import HtmlReporter
import sys


html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
phase = int(sys.argv[1])
area = int(sys.argv[4])
user = int(sys.argv[3])
table = '"HECHOS"."TTHECHOS_MOVIMIENTO"'

with define_extraction(phase, postgres_pool, postgres_pool) as (postgres, _):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase=phase, area=area, usuario=user, term=term_id):
        # Extracción
        truncate_table(postgres, 'TTHECHOS_MOVIMIENTO', term=term_id)
        extract_dataset_spark(configure_mit_spark, configure_postgres_spark, """
        SELECT DT.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               DT.FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               DT.FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
               DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
               DT.FTC_FOLIO AS FTC_FOLIO,
               DT.FNN_ID_REFERENCIA AS FTN_REFERENCIA,
               DT.FTF_MONTO_ACCIONES AS FTF_MONTO_ACCIONES,
               ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
               DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION,
               SUA.FND_FECHA_VALOR_RCV AS FTD_SUA_FECHA_VALOR_RCV,
               SUA.FND_FECHA_PAGO AS FTD_SUA_FECHA_PAGO,
               SUA.FNN_ULTIMO_SALARIO_INT_PER AS FTN_SUA_ULTIMO_SALARIO_INT_PER,
               SUA.FNN_DIAS_COTZDOS_BIMESTRE AS FTN_SUA_DIAS_COTZDOS_BIMESTRE,
               SUA.FNN_DIAS_AUSENT_BIMESTRE AS FTN_SUA_DIAS_AUSENT_BIMESTRE,
               SUA.FNN_DIAS_INCAP_BIMESTRE AS FTN_SUA_DIAS_INCAP_BIMESTRE,
               SUA.FNC_RFC_PATRON AS FTC_SUA_RFC_PATRON,
               SUA.FNC_REG_PATRONAL_IMSS AS FTC_SUA_REG_PATRONAL_IMSS,
               SUA.FNN_SECLOT AS FTN_SUA_SECLOT,
               SUA.FND_FECTRA AS FTD_SUA_FECTRA, 
               'M' AS FTC_BD_ORIGEN
        FROM TTAFOGRAL_MOV_AVOL DT
        LEFT JOIN CIERREN.TNAFORECA_SUA SUA ON SUA.FTC_FOLIO = DT.FTC_FOLIO AND SUA.FNN_ID_REFERENCIA = DT.FNN_ID_REFERENCIA
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, table, term=term_id, params={"start": start_month, "end": end_month})
        extract_dataset_spark(configure_mit_spark, configure_postgres_spark, """
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
        FROM TTAFOGRAL_MOV_BONO
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, table, term=term_id, params={"start": start_month, "end": end_month})
        extract_dataset_spark(configure_mit_spark, configure_postgres_spark, """
        SELECT DT.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               DT.FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               DT.FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
               DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
               DT.FTC_FOLIO AS FTC_FOLIO,
               DT.FNN_ID_REFERENCIA AS FTN_REFERENCIA,
               DT.FTF_MONTO_ACCIONES AS FTF_MONTO_ACCIONES,
               ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
               DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION,
               SUA.FND_FECHA_VALOR_RCV AS FTD_SUA_FECHA_VALOR_RCV,
               SUA.FND_FECHA_PAGO AS FTD_SUA_FECHA_PAGO,
               SUA.FNN_ULTIMO_SALARIO_INT_PER AS FTN_SUA_ULTIMO_SALARIO_INT_PER,
               SUA.FNN_DIAS_COTZDOS_BIMESTRE AS FTN_SUA_DIAS_COTZDOS_BIMESTRE,
               SUA.FNN_DIAS_AUSENT_BIMESTRE AS FTN_SUA_DIAS_AUSENT_BIMESTRE,
               SUA.FNN_DIAS_INCAP_BIMESTRE AS FTN_SUA_DIAS_INCAP_BIMESTRE,
               SUA.FNC_RFC_PATRON AS FTC_SUA_RFC_PATRON,
               SUA.FNC_REG_PATRONAL_IMSS AS FTC_SUA_REG_PATRONAL_IMSS,
               SUA.FNN_SECLOT AS FTN_SUA_SECLOT,
               SUA.FND_FECTRA AS FTD_SUA_FECTRA, 
               'M' AS FTC_BD_ORIGEN
        FROM TTAFOGRAL_MOV_COMP DT
        LEFT JOIN  CIERREN.TNAFORECA_SUA SUA ON SUA.FTC_FOLIO = DT.FTC_FOLIO AND SUA.FNN_ID_REFERENCIA = DT.FNN_ID_REFERENCIA
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, table, term=term_id, params={"start": start_month, "end": end_month})
        extract_dataset_spark(configure_mit_spark, configure_postgres_spark, """
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
        FROM TTAFOGRAL_MOV_GOB
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, table, term=term_id, params={"start": start_month, "end": end_month})
        extract_dataset_spark(configure_mit_spark, configure_postgres_spark, """
        SELECT DT.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               DT.FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               DT.FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               DT.FCN_ID_TIPO_SUBCTA AS FCN_ID_TIPO_SUBCTA,
               DT.FCN_ID_SIEFORE AS FCN_ID_SIEFORE,
               DT.FTC_FOLIO AS FTC_FOLIO,
               DT.FNN_ID_REFERENCIA AS FTN_REFERENCIA,
               DT.FTF_MONTO_ACCIONES AS FTF_MONTO_ACCIONES,
               ROUND(DT.FTF_MONTO_PESOS, 2) AS FTF_MONTO_PESOS,
               DT.FTD_FEH_LIQUIDACION AS FTD_FEH_LIQUIDACION,
               SUA.FND_FECHA_VALOR_RCV AS FTD_SUA_FECHA_VALOR_RCV,
               SUA.FND_FECHA_PAGO AS FTD_SUA_FECHA_PAGO,
               SUA.FNN_ULTIMO_SALARIO_INT_PER AS FTN_SUA_ULTIMO_SALARIO_INT_PER,
               SUA.FNN_DIAS_COTZDOS_BIMESTRE AS FTN_SUA_DIAS_COTZDOS_BIMESTRE,
               SUA.FNN_DIAS_AUSENT_BIMESTRE AS FTN_SUA_DIAS_AUSENT_BIMESTRE,
               SUA.FNN_DIAS_INCAP_BIMESTRE AS FTN_SUA_DIAS_INCAP_BIMESTRE,
               SUA.FNC_RFC_PATRON AS FTC_SUA_RFC_PATRON,
               SUA.FNC_REG_PATRONAL_IMSS AS FTC_SUA_REG_PATRONAL_IMSS,
               SUA.FNN_SECLOT AS FTN_SUA_SECLOT,
               SUA.FND_FECTRA AS FTD_SUA_FECTRA, 
               'M' AS FTC_BD_ORIGEN
        FROM TTAFOGRAL_MOV_RCV DT
        LEFT JOIN  CIERREN.TNAFORECA_SUA SUA ON SUA.FTC_FOLIO = DT.FTC_FOLIO AND SUA.FNN_ID_REFERENCIA = DT.FNN_ID_REFERENCIA
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, table, term=term_id, params={"start": start_month, "end": end_month})
        extract_dataset_spark(configure_mit_spark, configure_postgres_spark, """
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
        FROM TTAFOGRAL_MOV_SAR
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, table, term=term_id, params={"start": start_month, "end": end_month})
        extract_dataset_spark(configure_mit_spark, configure_postgres_spark, """
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
        FROM TTAFOGRAL_MOV_VIV
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, table, term=term_id, params={"start": start_month, "end": end_month})

        # Cifras de control
        report1 = html_reporter.generate(
            postgres,
            """
            SELECT I."FTC_GENERACION" AS GENERACION,
                   I."FTC_VIGENCIA" AS VIGENCIA,
                   I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                   I."FTC_ORIGEN" AS ORIGEN,
                   MC."FTC_DESCRIPCION" AS CONSAR,
                   COUNT(DISTINCT M."FCN_CUENTA") AS CLIENTES,
                   SUM(M."FTF_MONTO_PESOS") AS IMPORTE
            FROM "TTHECHOS_MOVIMIENTO" M
                INNER JOIN "TCHECHOS_CLIENTE" I ON M."FCN_CUENTA" = i."FCN_CUENTA" AND i."FCN_ID_PERIODO" = :term
                INNER JOIN "TTGESPRO_MOV_PROFUTURO_CONSAR" PC ON M."FCN_ID_TIPO_MOVIMIENTO" = PC."FCN_ID_MOVIMIENTO_PROFUTURO"
                INNER JOIN "TCDATMAE_MOVIMIENTO_CONSAR" MC ON PC."FCN_ID_MOVIMIENTO_CONSAR" = mc."FTN_ID_MOVIMIENTO_CONSAR"
            GROUP BY I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN", MC."FTC_DESCRIPCION"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Cliente", "Indicador Afiliación", "CONSAR"],
            ["Registros", "Importe"],
            params={"term": term_id},
        )

        report2 = html_reporter.generate(
            postgres,
            """
            --movimientos postgres
            SELECT
            g."FTC_PERIODO" AS PERIODO,
            s."FTC_DESCRIPCION" AS SIEFORE,
            sb."FCC_VALOR" AS SUBCUENTA,
            ROUND(cast(SUM (m."FTF_MONTO_PESOS") as numeric(16,2)),2) as MONTO_PESOS
            FROM "HECHOS"."TTHECHOS_MOVIMIENTO" m
            INNER JOIN "MAESTROS"."TCDATMAE_SIEFORE" s ON m."FCN_ID_SIEFORE" = s."FTN_ID_SIEFORE"
            --INNER JOIN "GESTOR"."TCGESPRO_MOVIMIENTO_PROFUTURO" mp ON mp."FTN_ID_MOVIMIENTO_PROFUTURO" = m."FCN_ID_CONCEPTO_MOVIMIENTO"
            INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" sb ON m."FCN_ID_TIPO_SUBCTA" = sb."FTN_ID_TIPO_SUBCTA"
            INNER JOIN "GESTOR"."TCGESPRO_PERIODO" g ON g."FTN_ID_PERIODO" = m."FCN_ID_PERIODO"
            WHERE "FCN_ID_PERIODO" = :term
            GROUP BY
            g."FTC_PERIODO", s."FTC_DESCRIPCION", sb."FCC_VALOR"
            ORDER BY
            s."FTC_DESCRIPCION", sb."FCC_VALOR"
            """,
            ["PERIODO", "SIEFORE", "SUBCUENTA"],
            ["MONTO_PESOS"],
            params={"term": term_id},
        )

        notify(
            postgres,
            "Cifras de control movimientos generadas",
            f"Se han generado las cifras de control para comisiones exitosamente para el periodo {time_period}",
            report1,
            term=term_id,
            control=True,
            area=area
        )
        notify(
            postgres,
            "Cifras de control movimientos generadas",
            f"Se han generado las cifras de control para comisiones exitosamente para el periodo {time_period}",
            report2,
            term=term_id,
            control=True,
            area=area
        )

