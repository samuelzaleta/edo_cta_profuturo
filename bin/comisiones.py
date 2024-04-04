from profuturo.common import truncate_table, notify, register_time, define_extraction
from profuturo.database import get_postgres_pool, get_postgres_oci_pool, configure_mit_spark, configure_postgres_oci_spark, configure_postgres_spark
from profuturo.extraction import extract_terms, extract_dataset_spark, read_table_insert_temp_view
from profuturo.reporters import HtmlReporter
from sqlalchemy import text
import sys
from datetime import datetime

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

users = (
10000851, 10000861, 10000868, 10000872, 1330029515, 1350011161, 1530002222, 1700004823, 3070006370, 3200089837,
3200231348, 3200534369, 3201895226, 3201900769, 3202077144, 3202135111, 3300118473, 3300576485, 3300797221,
3300809724,
3400764001, 3500053269, 3500058618, 6120000991, 6442107959, 6442128265, 6449009395, 6449015130, 10000884, 10000885,
10000887, 10000888, 10000889, 10000890, 10000891, 10000892, 10000893, 10000894, 10000895, 10000896, 10001041,
10001042,
10000898, 10000899, 10000900, 10000901, 10000902, 10000903, 10000904, 10000905, 10000906, 10000907, 10000908,
10000909,
10000910, 10000911, 10000912, 10000913, 10000914, 10000915, 10000916, 10000917, 10000918, 10000919, 10000920,
10000921,
10000922, 10000923, 10000924, 10000925, 10000927, 10000928, 10000929, 10000930, 10000931, 10000932, 10000933,
10000934,
10000935, 10000936, 10001263, 10001264, 10001265, 10001266, 10001267, 10001268, 10001269, 10001270, 10001271,
10001272,
10001274, 10001275, 10001277, 10001278, 10001279, 10001280, 10001281, 10001282, 10001283, 10001284, 10001285,
10001286,
10001288, 10001289, 10001290, 10001292, 10001293, 10001294, 10001296, 10001297, 10001298, 10001299, 10001300,
10001301,
10001305, 10001306, 10001307, 10001308, 10001309, 10001311, 10001312, 10001314, 10001315, 10001316, 10001317,
10001318,
10001319, 10001320, 10001321, 10001322, 10000896, 10000898, 10000790, 10000791, 10000792, 10000793, 10000794,
10000795,
10000797, 10000798, 10000799, 10000800, 10000801, 10000802, 10000803, 10000804, 10000805, 10000806, 10000807,
10000808,
10000809, 10000810, 10000811, 10000812, 10000813, 10000814, 10000815, 10000816, 10000817, 10000818, 10000819,
10000820,
10000821, 10000822, 10000823, 10000824, 10000825, 10000826, 10000827, 10000828, 10000830, 10000832, 10000833,
10000834,
10000835, 10000836, 10000837, 10000838, 10000839, 10000840, 10001098, 10001099, 10001100, 10001101, 10001102,
10001103,
10001104, 10001105, 10001106, 10001107, 10001108, 10001109, 10001110, 10001111, 10001112, 10001113, 10001114,
10001115,
10001116, 10001117, 10001118, 10001119, 10001120, 10001121, 10001122, 10001123, 10001124, 10001125, 10001126,
10001127,
10001128, 10001129, 10001130, 10001131, 10001132, 10001133, 10001134, 10001135, 10001136, 10001137, 10001138,
10001139,
10001140, 10001141, 10001142, 10001143, 10001145, 10001146, 10001147, 10001148, 10000991, 10000992, 10000993,
10000994,
10000995, 10000996, 10000997, 10000998, 10000999, 10001000, 10001001, 10001002, 10001003, 10001004, 10001005,
10001006,
10001007, 10001008, 10001009, 10001010, 10001011, 10001012, 10001013, 10001014, 10001015, 10001016, 10001017,
10001018,
10001019, 10001020, 10001021, 10001023, 10001024, 10001025, 10001026, 10001027, 10001029, 10001030, 10001031,
10001032,
10001033, 10001034, 10001035, 10001036, 10001037, 10001038, 10001039, 10001040, 1250002546, 3300005489, 3200653979,
3200442678, 3300056170, 3500058618, 1330029515, 1350011161, 1530002222, 3070006370, 3200089837, 3200474366,
3200534369,
3200767640, 3200840759, 3201096947, 3201292580, 3201900769, 1250002546, 3300005489, 3200653979, 3200442678,
3300056170,
10000851, 10000861, 10000868, 10000872, 1330029515, 1350011161, 1530002222, 1700004823, 3070006370, 3200089837,
3200231348, 3200534369, 3201895226, 3201900769, 3202077144, 3202135111, 3300118473, 3300576485, 3300797221, 3300809724,
3400764001, 3500053269, 3500058618, 6120000991, 6442107959, 6442128265, 6449009395, 6449015130, 10000884, 10000885,
10000887, 10000888, 10000889, 10000890, 10000891, 10000892, 10000893, 10000894, 10000895, 10000896, 10001041, 10001042,
10000898, 10000899, 10000900, 10000901, 10000902, 10000903, 10000904, 10000905, 10000906, 10000907, 10000908, 10000909,
10000910, 10000911, 10000912, 10000913, 10000914, 10000915, 10000916, 10000917, 10000918, 10000919, 10000920, 10000921,
10000922, 10000923, 10000924, 10000925, 10000927, 10000928, 10000929, 10000930, 10000931, 10000932, 10000933, 10000934,
10000935, 10000936, 10001263, 10001264, 10001265, 10001266, 10001267, 10001268, 10001269, 10001270, 10001271, 10001272,
10001274, 10001275, 10001277, 10001278, 10001279, 10001280, 10001281, 10001282, 10001283, 10001284, 10001285, 10001286,
10001288, 10001289, 10001290, 10001292, 10001293, 10001294, 10001296, 10001297, 10001298, 10001299, 10001300, 10001301,
10001305, 10001306, 10001307, 10001308, 10001309, 10001311, 10001312, 10001314, 10001315, 10001316, 10001317, 10001318,
10001319, 10001320, 10001321, 10001322, 10000896, 10000898, 10000790, 10000791, 10000792, 10000793, 10000794, 10000795,
10000797, 10000798, 10000799, 10000800, 10000801, 10000802, 10000803, 10000804, 10000805, 10000806, 10000807, 10000808,
10000809, 10000810, 10000811, 10000812, 10000813, 10000814, 10000815, 10000816, 10000817, 10000818, 10000819, 10000820,
10000821, 10000822, 10000823, 10000824, 10000825, 10000826, 10000827, 10000828, 10000830, 10000832, 10000833, 10000834,
10000835, 10000836, 10000837, 10000838, 10000839, 10000840, 10001098, 10001099, 10001100, 10001101, 10001102, 10001103,
10001104, 10001105, 10001106, 10001107, 10001108, 10001109, 10001110, 10001111, 10001112, 10001113, 10001114, 10001115,
10001116, 10001117, 10001118, 10001119, 10001120, 10001121, 10001122, 10001123, 10001124, 10001125, 10001126, 10001127,
10001128, 10001129, 10001130, 10001131, 10001132, 10001133, 10001134, 10001135, 10001136, 10001137, 10001138, 10001139,
10001140, 10001141, 10001142, 10001143, 10001145, 10001146, 10001147, 10001148, 10000991, 10000992, 10000993, 10000994,
10000995, 10000996, 10000997, 10000998, 10000999, 10001000, 10001001, 10001002, 10001003, 10001004, 10001005, 10001006,
10001007, 10001008, 10001009, 10001010, 10001011, 10001012, 10001013, 10001014, 10001015, 10001016, 10001017, 10001018,
10001019, 10001020, 10001021, 10001023, 10001024, 10001025, 10001026, 10001027, 10001029, 10001030, 10001031, 10001032,
10001033, 10001034, 10001035, 10001036, 10001037, 10001038, 10001039, 10001040, 3200089837, 3201423324, 3201693866,
3202486462, 3300118473, 3300780661, 3300809724, 3300835243, 3400764001, 6120000991, 6130000050, 6442107959,
6449015130, 6449061689, 6449083099, 8051533577, 8052970237, 1700004823, 3500058618, 3200231348, 3300576485,
3500053269, 1530002222, 3200840759, 3201292580, 3202135111, 8052710429, 3202077144, 3200474366, 3200767640,
3300797020, 3300797221, 3400958595, 3201900769, 3201895226, 3200534369, 1350011161, 3200996343, 1330029515,
3200976872, 3201368726, 3070006370, 6449009395, 6442128265, 3201096947
)

with define_extraction(phase, area, postgres_pool, postgres_oci_pool) as (postgres, postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]

    with register_time(postgres_pool, phase, term_id, user, area):
        # Extracción de tablas temporales
        query_temp = """
        SELECT
        "FTN_ID_TIPO_SUBCTA", "FCN_ID_REGIMEN", "FCN_ID_CAT_SUBCTA", "FCC_VALOR", "FTC_TIPO_CLIENTE"
        FROM "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"
        """
        extract_dataset_spark(
            configure_postgres_spark,
            configure_postgres_oci_spark,
            query_temp,
            '"MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"',
            term=term_id
        )

        # Extracción
        query = f"""
        SELECT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               C.FCN_ID_CONCEPTO_MOV AS FCN_ID_CONCEPTO_MOVIMIENTO,
               C.FTN_ID_MOV AS FCN_ID_MOVIMIENTO,
               C.FCN_ID_TIPO_MOV AS FCN_ID_TIPO_MOVIMIENTO,
               C.FCN_ID_SIEFORE,
               C.FTC_FOLIO,
               C.FTF_MONTO_ACCIONES,
               C.FTD_FEH_LIQUIDACION,
               C.FTF_MONTO_PESOS,
               S.FCN_ID_TIPO_SUBCTA as FTN_TIPO_SUBCTA
        FROM CIERREN.TTAFOGRAL_MOV_CMS C
        INNER JOIN CIERREN.TFCRXGRAL_CONFIG_MOV_ITGY M
        ON C.FCN_ID_CONCEPTO_MOV =M.FFN_ID_CONCEPTO_MOV
        INNER JOIN TRAFOGRAL_MOV_SUBCTA S ON M.FRN_ID_MOV_SUBCTA = S.FRN_ID_MOV_SUBCTA
        WHERE C.FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        and FTN_NUM_CTA_INVDUAL in {users}
        """
        truncate_table(postgres_oci, "TTHECHOS_COMISION", term=term_id)
        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_oci_spark,
            query,
            '"HECHOS"."TTHECHOS_COMISION"',
            term=term_id,
            params={"start": start_month, "end": end_month}
        )
        
        # Cifras de control
        report = html_reporter.generate(
            postgres_oci,
            """
            SELECT I."FTC_GENERACION" AS GENERACION,
                   I."FTC_VIGENCIA" AS VIGENCIA,
                   I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                   I."FTC_ORIGEN" AS ORIGEN,
                   S."FCC_VALOR" AS SUBCUENTA,
                   ROUND(SUM(C."FTF_MONTO_PESOS")::numeric, 2) AS COMISIONES
            FROM "HECHOS"."TTHECHOS_COMISION" C
                INNER JOIN "HECHOS"."TCHECHOS_CLIENTE" I ON C."FCN_CUENTA" = I."FCN_CUENTA" AND C."FCN_ID_PERIODO" = I."FCN_ID_PERIODO"
                INNER JOIN "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA" S ON C."FTN_TIPO_SUBCTA" = S."FTN_ID_TIPO_SUBCTA"
            WHERE C."FCN_ID_PERIODO" = :term
            GROUP BY  I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN", S."FCC_VALOR"
            """,
            ["Tipo Generación", "Vigencia", "Tipo Formato", "Indicador Afiliación", "SUBCUENTA"],
            ["Monto_Comisiones"],
            params={"term": term_id},
        )



        notify(
            postgres,
            f"Comisiones",
            phase,
            area,
            term=term_id,
            message=f"Se han ingestado las comisiones de forma exitosa para el periodo",
            details=report,
        )
        #Elimina tablas temporales
        postgres_oci.execute(text("""
        DROP TABLE IF EXISTS "MAESTROS"."TCDATMAE_TIPO_SUBCUENTA"
        """))
