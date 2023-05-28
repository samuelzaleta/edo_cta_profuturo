from common import MemoryLogger, HtmlReporter, get_postgres_pool, get_mit_pool, get_buc_pool, truncate_table, extract_terms, extract_dataset, notify


app_logger = MemoryLogger()
html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
mit_pool = get_mit_pool()
buc_pool = get_buc_pool()

with postgres_pool.begin() as postgres:
    terms = extract_terms(postgres)

    with mit_pool.begin() as mit:
        for term in terms:
            term_id = term["id"]
            start_month = term["start_month"]
            end_month = term["end_month"]

            # Extracción
            truncate_table(postgres, 'tthechos_movimientos', term_id)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_AVOL
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "tthechos_movimientos", term_id, phase=3, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_BONO
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "tthechos_movimientos", term_id, phase=3, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_COMP
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "tthechos_movimientos", term_id, phase=3, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_GOB
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "tthechos_movimientos", term_id, phase=3, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_RCV
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "tthechos_movimientos", term_id, phase=3, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_SAR
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "tthechos_movimientos", term_id, phase=3, params={"start": start_month, "end": end_month}, limit=100)
            extract_dataset(mit, postgres, """
            SELECT FTN_NUM_CTA_INVDUAL AS fcn_cuenta,
                   FCN_ID_TIPO_MOV,
                   FCN_ID_CONCEPTO_MOV,
                   FCN_ID_TIPO_SUBCTA,
                   FCN_ID_SIEFORE,
                   FTC_FOLIO,
                   FTF_MONTO_ACCIONES,
                   FTF_MONTO_PESOS,
                   FTD_FEH_LIQUIDACION
            FROM TTAFOGRAL_MOV_VIV
            WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
            """, "tthechos_movimientos", term_id, phase=3, params={"start": start_month, "end": end_month}, limit=100)

            # Cifras de control
            report = html_reporter.generate(
                postgres,
                """
                SELECT ftc_generacion,
                       ftc_vigencia,
                       CASE
                           WHEN ftc_origen = 'Asignado' THEN 'Asignado'
                           WHEN ftb_pension = true AND ftb_bono = true THEN 'Pensionado con Bono'
                           WHEN ftb_pension = true THEN 'Pensionado'
                           WHEN ftc_origen = 'Afiliado' AND ftb_bono = true THEN 'Afiliado con Bono'
                           WHEN ftc_origen = 'Afiliado' THEN 'Afiliado'
                       END AS tipo_formato,
                       ftc_tipo_cliente,
                       mc.ftc_descripcion,
                       COUNT(DISTINCT c.ftn_cuenta) AS clientes,
                       SUM(m.ftf_monto_pesos) AS importe
                FROM tthechos_movimientos m
                    INNER JOIN ttgespro_mov_profuturo_consar pc ON m.fcn_id_concepto_mov = pc.fcc_id_movimiento_profuturo
                    INNER JOIN tcdatmae_movimientos_consar mc on pc.fcn_id_movimiento_consar = mc.ftn_id_movimiento_consar
                    INNER JOIN tcdatmae_clientes c on m.fcn_cuenta = c.ftn_cuenta
                GROUP BY ftc_generacion,
                         ftc_vigencia,
                         CASE
                             WHEN ftc_origen = 'Asignado' THEN 'Asignado'
                             WHEN ftb_pension = true AND ftb_bono = true THEN 'Pensionado con Bono'
                             WHEN ftb_pension = true THEN 'Pensionado'
                             WHEN ftc_origen = 'Afiliado' AND ftb_bono = true THEN 'Afiliado con Bono'
                             WHEN ftc_origen = 'Afiliado' THEN 'Afiliado'
                         END,
                         ftc_tipo_cliente,
                         mc.ftc_descripcion
                """,
                ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SIEFORE"],
                ["Registros", "Comisiones"],
            )

            notify(
                postgres,
                term_id,
                "Cifras de control Comisiones generadas",
                "Se han generado las cifras de control para comisiones exitosamente",
                report,
                control=True,
            )
