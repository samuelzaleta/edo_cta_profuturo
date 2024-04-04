from profuturo.common import register_time, define_extraction, notify, truncate_table
from profuturo.database import get_postgres_pool, get_postgres_oci_pool, configure_buc_spark, configure_mit_spark, configure_postgres_spark, configure_postgres_oci_spark, configure_postgres_spark
from profuturo.extraction import _get_spark_session, _write_spark_dataframe, read_table_insert_temp_view, extract_dataset_spark
from profuturo.reporters import HtmlReporter
from profuturo.extraction import extract_terms
from sqlalchemy import text
from datetime import datetime
import sys

html_reporter = HtmlReporter()
postgres_pool = get_postgres_pool()
postgres_oci_pool = get_postgres_oci_pool()

phase = int(sys.argv[1])
user = int(sys.argv[3])
area = int(sys.argv[4])

with define_extraction(phase, area, postgres_pool,postgres_oci_pool,) as (postgres,postgres_oci):
    term = extract_terms(postgres, phase)
    term_id = term["id"]
    time_period = term["time_period"]
    start_month = term["start_month"]
    end_month = term["end_month"]
    spark = _get_spark_session(
        excuetor_memory='18g',
        memory_overhead='1g',
        memory_offhead='1g',
        driver_memory='2g',
        intances=4,
        parallelims=8000)
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

    with register_time(postgres_pool, phase, term_id, user, area):
        # Extracción
        query = f"""
        SELECT
            FTN_CUENTA,
            FTC_NOMBRE,
            FTC_AP_PATERNO,
            FTC_AP_MATERNO,
            FTC_CALLE,
            FTC_ASENTAMIENTO,
            TRIM(CONCAT(CONCAT(NUMEROEXTERIOR,' '),NUMEROINTERIOR)) AS FTC_NUMERO,
            FTC_COLONIA,
            FTC_DELEGACION,
            FTC_MUNICIPIO,
            FTC_CODIGO_POSTAL,
            FTC_ENTIDAD_FEDERATIVA,
            FTC_NSS,
            FTC_CURP,
            FTC_RFC 
        FROM (
            SELECT
               ROW_NUMBER() over (partition by TO_NUMBER(REGEXP_REPLACE(TO_CHAR(C.NUMERO), '[^0-9]', '')) order by C.NUMERO) AS id,
               TO_NUMBER(REGEXP_REPLACE(TO_CHAR(C.NUMERO), '[^0-9]', '')) AS FTN_CUENTA,
               PF.NOMBRE AS FTC_NOMBRE,
               PF.APELLIDOPATERNO AS FTC_AP_PATERNO,
               PF.APELIDOMATERNO AS FTC_AP_MATERNO,
               CASE 
               WHEN DI.ASENTAMIENTO IS NULL THEN ASE.NOMBRE 
               ELSE DI.ASENTAMIENTO
               END FTC_ASENTAMIENTO, 
               DI.CALLE AS FTC_CALLE,
               CASE 
               WHEN DI.NUMEROEXTERIOR IS NOT NULL THEN DI.NUMEROEXTERIOR
               ELSE DI.NUMEROEXTERIOR
               END NUMEROEXTERIOR,
               CASE 
                WHEN DI.NUMEROINTERIOR IS NOT NULL THEN DI.NUMEROINTERIOR
                ELSE DI.NUMEROINTERIOR
                END NUMEROINTERIOR,
               ASE.NOMBRE AS FTC_COLONIA,
               CD.NOMBRE AS FTC_DELEGACION,
               M.NOMBRE AS FTC_MUNICIPIO,
               concat('C.P. ',CP.CODIGOPOSTAL) AS FTC_CODIGO_POSTAL,
               E.NOMBRE AS FTC_ENTIDAD_FEDERATIVA,
               NSS.VALOR_IDENTIFICADOR AS FTC_NSS,
               CURP.VALOR_IDENTIFICADOR AS FTC_CURP,
               RFC.VALOR_IDENTIFICADOR AS FTC_RFC
            FROM CONTRATO C
                INNER JOIN PERSONA_CONT_ROL PCR ON C.IDCONTRATO = PCR.IDCONTRATO
                INNER JOIN PERSONA_FISICA PF ON PCR.IDPERSONA = PF.IDPERSONA
                INNER JOIN DOMICILIO D ON PF.IDPERSONA = D.IDPERSONA
                INNER JOIN DIRECCION DI ON D.IDDIRECCION = DI.IDDIRECCION
                LEFT JOIN CODIGO_POSTAL CP ON DI.IDCODIGOPOSTAL = CP.IDCODIGOPOSTAL
                LEFT JOIN ESTADO E ON CP.IDESTADO = E.IDESTADO
                LEFT JOIN MUNICIPIO M ON CP.IDMUNICIPIO = M.IDMUNICIPIO
                LEFT JOIN CIUDAD CD ON CP.IDCIUDAD = CD.IDCIUDAD
                LEFT JOIN ASENTAMIENTO ASE ON DI.IDASENTAMIENTO = ASE.IDASENTAMIENTO
                LEFT JOIN IDENTIFICADOR NSS ON PF.IDPERSONA = NSS.IDPERSONA AND NSS.IDTIPOIDENTIFICADOR = 3 
                                           AND NSS.VALIDO = 1 AND NSS.ACTIVO = 1
                LEFT JOIN IDENTIFICADOR CURP ON PF.IDPERSONA = CURP.IDPERSONA AND CURP.IDTIPOIDENTIFICADOR = 2 
                                            AND CURP.VALIDO = 1 AND CURP.ACTIVO = 1
                LEFT JOIN IDENTIFICADOR RFC ON PF.IDPERSONA = RFC.IDPERSONA AND RFC.IDTIPOIDENTIFICADOR = 1 
                                           AND RFC.VALIDO = 1 AND RFC.ACTIVO = 1
            WHERE PCR.IDROL = 787 -- Rol cliente
              AND C.IDLINEANEGOCIO = 763 -- Linea de negocio
              AND D.IDTIPODOM = 818 -- Tipo de domicilio Particular
              AND D.IDSTATUSDOM = 761 -- ACTIVO
              -- AND D.PREFERENTE = 1 Domicilio preferente
        ) where id = 1
        and FTN_CUENTA in {users}
        """
        queryCorreo = f"""
        SELECT 
        FTN_CUENTA AS FCN_CUENTA, FTC_CORREO_ELEC AS FTC_CORREO,
        COALESCE(FTC_TEL_PREF,FTC_TEL_OFICINA,FTC_TEL_FIJO,FTN_CELULAR,FTC_TEL_RECADO) AS FTC_TELEFONO   
        FROM (SELECT FTN_CUENTA, IDPERSONA AS FTN_IDPERSONA,  FTC_CORREO_PREFERENTE,
        (CASE WHEN EMAIL_PREFERENTE IS NOT  NULL THEN EMAIL_PREFERENTE
            ELSE 
                     CASE WHEN CORREO1 IS NOT NULL THEN CORREO1
                     WHEN (CORREO2 IS NOT NULL  AND CORREO1 IS  NULL) THEN CORREO2
                     ELSE EMAIL_PREFERENTE END
                     END) AS FTC_CORREO_ELEC,  
        FTC_TEL_PREF,  FTC_TELEFONO_PREFERENTE, FTC_TEL_OFICINA, FTC_TEL_FIJO ,   FTN_CELULAR,  FTC_TEL_RECADO 
        FROM (
        SELECT FTN_CUENTA,IDPERSONA, MAX(EMAIL_PREFERENTE) AS EMAIL_PREFERENTE, MAX(FTC_CORREO_PREFERENTE)AS FTC_CORREO_PREFERENTE, MAX(CORREO1) AS CORREO1, MAX(CORREO2) AS CORREO2, 
              MAX(FTC_TEL_PREFERENTE) AS FTC_TEL_PREF, MAX(FTC_TELEFONO_PREFERENTE) AS FTC_TELEFONO_PREFERENTE, MAX (ftc_tel_fijo) AS FTC_TEL_FIJO,
            MAX (ftc_tel_oficina) AS FTC_TEL_OFICINA, MAX (ftn_celular) AS FTN_CELULAR, MAX ( FTC_TEL_RECADO) AS FTC_TEL_RECADO
        FROM 
        (        
            SELECT /*+PARALLEL(C 20) PARALLEL(E 20) PARALLEL(T 20) PARALLEL(TP_TEL 20) USE_HASH(MC) ORDERED */
                 TO_NUMBER(REGEXP_REPLACE(TO_CHAR(C.NUMERO), '[^0-9]', '')) AS FTN_CUENTA,
                  MCP.IDPERSONA, 
                            (CASE WHEN MC.PREFERENTE = 1 AND E.IDMEDIOCONTACTO IS NOT NULL THEN e.email  ELSE ''   END ) "EMAIL_PREFERENTE" , 
                            (CASE WHEN MC.PREFERENTE = 1 THEN '1' ELSE '0' END )as FTC_CORREO_PREFERENTE,
                            (CASE WHEN e.idtipoemail = 792 THEN e.email  ELSE ''   END) "CORREO1",
                            (CASE WHEN e.idtipoemail = 793 THEN e.email  ELSE ''   END) "CORREO2",                            
                            (CASE WHEN MC.PREFERENTE = 1 AND IDTIPOTELEFONO IS NOT NULL THEN (clavenacional || T.numero) ELSE ''  END ) "FTC_TEL_PREFERENTE" ,
                            (CASE WHEN MC.PREFERENTE = 1 AND IDTIPOTELEFONO IS NOT NULL THEN tp_tel.descripcion ELSE '' END ) "FTC_TELEFONO_PREFERENTE" ,
                            (CASE WHEN IDTIPOTELEFONO = 799 THEN (clavenacional || T.numero) ELSE '' END) "FTC_TEL_FIJO",
                            (CASE WHEN IDTIPOTELEFONO = 800 THEN (clavenacional || T.numero) ELSE '' END) "FTC_TEL_OFICINA",
                            (CASE WHEN IDTIPOTELEFONO = 801 THEN (clavenacional || T.numero) ELSE '' END) "FTN_CELULAR",
                            (CASE WHEN IDTIPOTELEFONO = 802 THEN (clavenacional || T.numero) ELSE '' END) "FTC_TEL_RECADO"
                FROM CLUNICO.MEDIO_CONT_PERSONA  MCP 
                  INNER JOIN CLUNICO.MEDIO_CONTACTO MC ON (MC.IDMEDIOCONTACTO = MCP.IDMEDIOCONTACTO and MC.IDSTATUSMCONTACTO = 757 )   
                  INNER JOIN CLUNICO.PERSONA_CONT_ROL pcr ON (MCP.IDPERSONA = PCR.IDPERSONA)
                  INNER JOIN CLUNICO.CONTRATO C ON (C.IDCONTRATO = PCR.IDCONTRATO)
                  LEFT  JOIN CLUNICO.EMAIL E ON (mcp.IDMEDIOCONTACTO = E.IDMEDIOCONTACTO AND INSTR(E.EMAIL,'@') > 0)  
                  LEFT  JOIN CLUNICO.TELEFONO T ON (mcp.IDMEDIOCONTACTO = T.IDMEDIOCONTACTO ) 
                  LEFT JOIN CLUNICO.catalogo_general TP_TEL ON (T.IDTIPOTELEFONO=TP_TEL.idcatalogogral)    
                WHERE mc.valido = 1
                AND  PCR.IDROL=787
                AND C.IDLINEANEGOCIO = 763
                  )            
                group by IDPERSONA, FTN_CUENTA
            )
          ) 
          
        WHERE FCN_CUENTA IN {user}
        """
        queryCliente = f"""
        SELECT C."FTN_CUENTA", C."FTC_CORREO", C."FTC_TELEFONO", X."INDICADOR"
        FROM "MAESTROS"."TCDATMAE_CLIENTE" C
        INNER JOIN (
        SELECT "FCN_CUENTA", 'correo' AS "INDICADOR"
        FROM "HECHOS"."TTHECHOS_CARGA_ARCHIVO"
        WHERE "FCN_ID_INDICADOR" IN (25, 27)
        UNION ALL
        SELECT "FCN_CUENTA", 'telefono' AS "INDICADOR"
        FROM "HECHOS"."TTHECHOS_CARGA_ARCHIVO"
        WHERE "FCN_ID_INDICADOR" IN (26)
        ) X ON X."FCN_CUENTA" = C."FTN_CUENTA"
        """

        read_table_insert_temp_view(
            configure_buc_spark,
            query=queryCorreo,
            view='correoTelefono',
        )

        read_table_insert_temp_view(
            configure_postgres_spark,
            query=queryCliente,
            view='clientePostgres',
        )

        read_table_insert_temp_view(
            configure_buc_spark,
            query=query,
            view='cliente',
        )

        df = spark.sql(f"""
        SELECT FCN_CUENTA FROM (SELECT 
        CT.FCN_CUENTA,
        CASE
            WHEN INDICADOR = 'correo' THEN cast(CT.FTC_CORREO as varchar(30)) = cast(CP.FTC_CORREO as varchar(30))
            ELSE CT.FTC_TELEFONO = CP.FTC_TELEFONO
        END AS FTB_EVALUACION
        FROM 
        correoTelefono CT
        INNER JOIN 
        clientePostgres CP ON CT.FCN_CUENTA = CP.FTN_CUENTA
        ) X
        WHERE FTB_EVALUACION = 'FALSE'
        and FCN_CUENTA in {users}
        """)

        rows = df.collect()

        tuple_list = [tuple(row) for row in rows]
        if len(tuple_list) > 0:
            postgres.execute(text("""
                        DELETE FROM "HECHOS"."TTHECHOS_CARGA_ARCHIVO"
                        WHERE "FCN_ID_INDICADOR" IN (25,27,26)
                        """), {"term": term_id, "user": tuple(tuple_list)})

        df = spark.sql(""" 
        SELECT c.*, ct.FTC_TELEFONO,
        ct.FTC_CORREO FROM cliente c
        LEFT JOIN  correoTelefono ct ON  c.FTN_CUENTA = ct.FCN_CUENTA 
         """).cache()

        truncate_table(postgres_oci, "TCDATMAE_CLIENTE")
        _write_spark_dataframe(df, configure_postgres_oci_spark, '"MAESTROS"."TCDATMAE_CLIENTE"')

        df.unpersist()

        # Extracción
        truncate_table(postgres, "TCHECHOS_CLIENTE_INDICADOR", term=term_id)
        truncate_table(postgres, "TCGESPRO_MUESTRA", term=term_id, area=area)
        truncate_table(postgres, "TCHECHOS_CLIENTE", term=term_id)
        read_table_insert_temp_view(configure_mit_spark, query=f"""
        SELECT
        PG.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
        'TRUE' AS FCC_VALOR
        FROM BENEFICIOS.TTCRXGRAL_PAGO PG
            INNER JOIN BENEFICIOS.TTCRXGRAL_PAGO_SUBCTA PGS
            ON PGS.FTC_FOLIO = PG.FTC_FOLIO AND PGS.FTC_FOLIO_LIQUIDACION = PG.FTC_FOLIO_LIQUIDACION
            INNER JOIN BENEFICIOS.TTAFORETI_TRAMITE TR
            ON TR.FTN_FOLIO_TRAMITE = PG.FTN_FOLIO_TRAMITE
        WHERE PG.FCN_ID_PROCESO IN (4050,4051)
                AND PG.FCN_ID_SUBPROCESO IN (309, 310, 330,331)
                AND PGS.FCN_ID_TIPO_SUBCTA NOT IN (15,16,17,18)
                AND TRUNC(PG.FTD_FEH_LIQUIDACION) <= :end
                AND PG.FTN_NUM_CTA_INVDUAL IN {users}
        """, view="indicador_pension", params={'end': end_month})

        read_table_insert_temp_view(configure_mit_spark, query=f"""
        SELECT DISTINCT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND
                   WHEN '66' THEN 'IMSS'
                   WHEN '67' THEN 'ISSSTE'
                   WHEN '68' THEN 'INDEPENDIENTE'
                   WHEN '69' THEN 'MIXTO'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
        INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE CONF.FFN_ID_CONFIG_INDI = 1
          AND FTC_VIGENCIA= 1
          AND IND.FTN_NUM_CTA_INVDUAL IN {users}
        """, view="indicador_origen")
        spark.sql("select count(*) as count_indicador_origen from indicador_origen").show()

        read_table_insert_temp_view(configure_mit_spark, query=f"""
        SELECT DISTINCT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND
                   WHEN '713' THEN 'Asignado'
                   WHEN '714' THEN 'Afiliado'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
        INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE CONF.FFN_ID_CONFIG_INDI = 12
          AND FTC_VIGENCIA= 1
          AND IND.FTN_NUM_CTA_INVDUAL IN {users}
        """, view="indicador_tipo_cliente")

        read_table_insert_temp_view(configure_mit_spark, query=f"""
        SELECT DISTINCT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
               CASE IND.FCC_VALOR_IND 
                   WHEN '1' THEN 'V'
                   WHEN '0' THEN 'N'
               END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
        INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
        WHERE CONF.FFN_ID_CONFIG_INDI = 2
          AND FTC_VIGENCIA= 1
          AND IND.FTN_NUM_CTA_INVDUAL IN {users}
        """, view="indicador_vigencia")

        read_table_insert_temp_view(configure_mit_spark, query=f"""
        SELECT DISTINCT FTN_NUM_CTA_INVDUAL AS FCN_CUENTA, 'TRUE' /* TRUE */ AS FCC_VALOR
        FROM CIERREN.THAFOGRAL_SALDO_HISTORICO_V2
        WHERE FCN_ID_SIEFORE = 81
        """, view="indicador_bono")

        read_table_insert_temp_view(configure_mit_spark, query=f"""
        SELECT
            PG.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
            CASE
            WHEN PG.FCN_ID_SUBPROCESO = 310 THEN 'Pensión garantizada'
            WHEN PG.FCN_ID_SUBPROCESO = 309 THEN 'Pensión garantizada'
            WHEN PG.FCN_ID_SUBPROCESO = 330 THEN 'Retiro programado'
            ELSE 'Retiro programado'
            END FCC_VALOR
        FROM BENEFICIOS.TTCRXGRAL_PAGO PG
            INNER JOIN BENEFICIOS.TTCRXGRAL_PAGO_SUBCTA PGS
            ON PGS.FTC_FOLIO = PG.FTC_FOLIO AND PGS.FTC_FOLIO_LIQUIDACION = PG.FTC_FOLIO_LIQUIDACION
            INNER JOIN BENEFICIOS.TTAFORETI_TRAMITE TR
            ON TR.FTN_FOLIO_TRAMITE = PG.FTN_FOLIO_TRAMITE
        WHERE PG.FCN_ID_PROCESO IN (4050,4051)
                AND PG.FCN_ID_SUBPROCESO IN (309, 310, 330,331)
                AND PGS.FCN_ID_TIPO_SUBCTA NOT IN (15,16,17,18)
                AND TRUNC(PG.FTD_FEH_LIQUIDACION) <= :end
                AND PG.FTN_NUM_CTA_INVDUAL IN {users}
        """, view="indicador_tipo_pension", params={'start': start_month, 'end': end_month})

        read_table_insert_temp_view(configure_mit_spark, query=f"""
        SELECT
        P.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
        CASE WHEN SUBSTR(C.FCC_VALOR,6,7) = 'BI' then 'Inicial'
             WHEN SUBSTR(C.FCC_VALOR,6,7) = 'BP' then 'De Pensiones'
             ELSE SUBSTR(C.FCC_VALOR, 6, 7) || '-' || TO_CHAR(CAST(SUBSTR(C.FCC_VALOR, 6, 7) AS INT) + 4)
             END FCC_VALOR
        FROM TTAFOGRAL_OSS P
        INNER JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO C ON P.FCN_ID_SIEFORE= C.FCN_ID_CAT_CATALOGO
        WHERE P.FTC_ESTATUS = 1 AND FTN_PRIORIDAD = 1 and P.FCN_ID_GRUPO = 141
        AND P.FTN_NUM_CTA_INVDUAL IN {users}
        """, view="indicador_perfil_inversion")

        read_table_insert_temp_view(configure_mit_spark, query=f"""
        SELECT DISTINCT IND.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
           CASE IND.FCC_VALOR_IND
               WHEN '10785' THEN 'AFORE'
               WHEN '10786' THEN 'TRANSICION'
               WHEN '10789' THEN 'MIXTO'
           END AS FCC_VALOR
        FROM TTAFOGRAL_IND_CTA_INDV IND
            INNER JOIN tfafogral_config_indi CONF ON IND.FFN_ID_CONFIG_INDI = CONF.FFN_ID_CONFIG_INDI
            WHERE CONF.FFN_ID_CONFIG_INDI = 34
            AND FTC_VIGENCIA= 1 
            AND IND.FTN_NUM_CTA_INVDUAL IN {users}
        """, view="indicador_generacion")

        df = spark.sql(f"""
        SELECT 
               DISTINCT 
               c.FTN_CUENTA as FCN_CUENTA,
               {term_id} AS FCN_ID_PERIODO,
               coalesce(cast(p.FCC_VALOR AS BOOLEAN), false) AS FTB_PENSION, 
               t.FCC_VALOR AS  FTC_TIPO_CLIENTE,
               o.FCC_VALOR AS FTC_ORIGEN,
               v.FCC_VALOR AS FTC_VIGENCIA,
               g.FCC_VALOR AS FTC_GENERACION,
               coalesce(cast(p.FCC_VALOR AS BOOLEAN), false)  AS FTB_BONO,
               tp.FCC_VALOR AS FTC_TIPO_PENSION,
               i.FCC_VALOR AS FTC_PERFIL_INVERSION
               --JSON_OBJECT('Vigencia', v.FCC_VALOR, 'Generacion', g.FCC_VALOR) AS FTO_INDICADORES
        FROM cliente c
            LEFT JOIN indicador_generacion g ON c.FTN_CUENTA = g.FCN_CUENTA
            LEFT JOIN indicador_origen o ON c.FTN_CUENTA = o.FCN_CUENTA
            LEFT JOIN indicador_tipo_cliente t ON c.FTN_CUENTA = t.FCN_CUENTA
            LEFT JOIN indicador_pension p ON c.FTN_CUENTA= p.FCN_CUENTA
            LEFT JOIN indicador_vigencia v ON c.FTN_CUENTA= v.FCN_CUENTA
            LEFT JOIN indicador_bono b ON c.FTN_CUENTA = b.FCN_CUENTA
            LEFT JOIN indicador_tipo_pension tp ON c.FTN_CUENTA = p.FCN_CUENTA
            LEFT JOIN indicador_perfil_inversion i ON c.FTN_CUENTA = i.FCN_CUENTA
        """)
        # df = df.withColumn("FTO_INDICADORES", to_json(struct(lit('{}'))))

        df = df.dropDuplicates(["FCN_CUENTA"])

        df = df.repartition(60)

        _write_spark_dataframe(df, configure_postgres_oci_spark, '"HECHOS"."TCHECHOS_CLIENTE"')

        postgres.execute(text("""
                UPDATE "HECHOS"."TCHECHOS_CLIENTE"
                SET "FTC_GENERACION" = 'DECIMO TRANSITORIO'
                WHERE "FCN_CUENTA" IN (SELECT "FCN_CUENTA" FROM "HECHOS"."TTHECHOS_CARGA_ARCHIVO" WHERE "FCN_ID_INDICADOR" = 28)
                            """), {"term": term_id, "area": area})

        postgres.execute(text("""
                UPDATE "HECHOS"."TCHECHOS_CLIENTE"
                SET "FTB_PENSION" = 'true'
                WHERE "FCN_CUENTA" IN (SELECT "FCN_CUENTA" FROM "HECHOS"."TTHECHOS_CARGA_ARCHIVO" WHERE "FCN_ID_INDICADOR" = 73)
                """), {"term": term_id, "area": area})

        query_pension = """
                SELECT
                PG.FTN_NUM_CTA_INVDUAL AS FCN_CUENTA,
                CASE
                WHEN PG.FCN_ID_SUBPROCESO = 310 THEN 'Pensión garantizada'
                WHEN PG.FCN_ID_SUBPROCESO = 309 THEN 'Pensión garantizada'
                WHEN PG.FCN_ID_SUBPROCESO = 330 THEN 'Retiro programado'
                ELSE 'Retiro programado'
                END FTC_TIPO_PENSION,
                SUM(PGS.FTN_MONTO_PESOS) AS FTN_MONTO_PEN
                FROM BENEFICIOS.TTCRXGRAL_PAGO PG
                    INNER JOIN BENEFICIOS.TTCRXGRAL_PAGO_SUBCTA PGS
                    ON PGS.FTC_FOLIO = PG.FTC_FOLIO AND PGS.FTC_FOLIO_LIQUIDACION = PG.FTC_FOLIO_LIQUIDACION
                    INNER JOIN BENEFICIOS.TTAFORETI_TRAMITE TR
                    ON TR.FTN_FOLIO_TRAMITE = PG.FTN_FOLIO_TRAMITE
                WHERE PG.FCN_ID_PROCESO IN (4050,4051)
                        AND PG.FCN_ID_SUBPROCESO IN (309, 310, 330,331)
                        AND PGS.FCN_ID_TIPO_SUBCTA NOT IN (15,16,17,18)
                        AND TRUNC(PG.FTD_FEH_LIQUIDACION) <= :end
                GROUP BY
                PG.FTN_NUM_CTA_INVDUAL,PG.FCN_ID_SUBPROCESO
                """

        truncate_table(postgres, "TCDATMAE_PENSION")

        extract_dataset_spark(
            configure_mit_spark,
            configure_postgres_spark,
            query_pension,
            '"MAESTROS"."TCDATMAE_PENSION"',
            params={"end": end_month, "type": "F"},
        )

        # Cifras de control
        report = html_reporter.generate(
            postgres,
            """
             SELECT I."FTC_GENERACION" AS GENERACION,
                    I."FTC_VIGENCIA" AS VIGENCIA,
                    I."FTC_TIPO_CLIENTE" AS TIPO_CLIENTE,
                    I."FTC_ORIGEN" AS ORIGEN,
                    COUNT(DISTINCT I."FCN_CUENTA") AS CLIENTES
             FROM "HECHOS"."TCHECHOS_CLIENTE" I
             WHERE I."FCN_ID_PERIODO" = :term
             GROUP BY I."FTC_GENERACION", I."FTC_VIGENCIA", I."FTC_TIPO_CLIENTE", I."FTC_ORIGEN"
             """,
            ["Tipo Generación", "Vigencia", "Tipo Cliente", "Indicador Afiliación"],
            ["Clientes"],
            params={"term": term_id},
        )

        notify(
            postgres,
            f"Clientes",
            phase,
            area,
            term=term_id,
            message=f"Se han ingestado los clientes de forma exitosa para el periodo",
            details=report,
        )