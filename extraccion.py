from sqlalchemy.engine.base import Connection
from sqlalchemy import text
from typing import Any, Dict
from datetime import datetime, date, timedelta
import oracledb
import psycopg2
import pandas as pd
import sqlalchemy
import calendar


def get_oracle_conn() -> oracledb.Connection:
    return oracledb.connect(
        host="172.22.180.190",
        service_name="mitafore.profuturo-gnp.net",
        wallet_location="/opt/wallet",
        user="PROFUTURO_QAMOD",
        password="Pa55w0rd*19",
    )


def get_buc_conn() -> oracledb.Connection:
    return oracledb.connect(
        host="qa34-scan.profuturo-gnp.net",
        port=16161,
        service_name="QA34",
        wallet_location="/opt/wallet",
        user="CLUNICO",
        password="temp4now13",
    )


def get_postgres_conn() -> psycopg2.Connection:
    return psycopg2.connect(
        host="34.72.193.129",
        user="postgres",
        password="%Xh@q/_-<V\"~1.8.",
    )


def extract_dataset(origin: Connection, destination: Connection, query: str, table: str, params: Dict[str, Any] = None, limit: int = None):
    if params is None:
        params = {}
    if limit is not None:
        query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
        params["limit"] = limit

    print(f"Extracting {table}...")

    service_level = timedelta(hours=2)

    start = datetime.now()
    df_pd = pd.read_sql_query(text(query), origin, params=params)
    df_pd.to_sql(table, destination, if_exists="replace")

    df_sp = spark.createDataFrame(df_pd)
    df_sp.write.parquet(f"gs://spark-dataproc-poc/Datalake/{table}", mode="overwrite")
    end = datetime.now()
    delta = end - start

    flag: str
    if delta <= service_level:
        flag = "Verde"
    elif delta <= service_level + timedelta(hours=1):
        flag = "Amarillo"
    else:
        flag = "Rojo"

    destination.execute(text("""
    INSERT INTO tcgespro_fase (descripcion, descripcion_corta, nivel_servicio, bandera, inicio_periodo, fin_periodo) 
    VALUES (:descripcion, :descripcion_corta, :nivel_servicio, :bandera, :inicio_periodo, :fin_periodo)
    """), {
        "descripcion": f"Extracción de tabla '{table}' desde Oracle a Postgres y Parquet",
        "descripcion_corta": f"Extracción de tabla '{table}'",
        "nivel_servicio": service_level.total_seconds(),
        "bandera": flag,
        "inicio_periodo": start,
        "fin_periodo": end,
    })

    print(f"Done extracting {table}!")


oracledb.init_oracle_client()

oracle_pool = sqlalchemy.create_engine(
    "oracle+oracledb://",
    creator=get_oracle_conn,
)
with oracle_pool.connect() as conn:
    conn.execute(text("SELECT 'OK' FROM dual"))
    print("Oracle connection OK")

buc_pool = sqlalchemy.create_engine(
    "oracle+oracledb://",
    creator=get_buc_conn,
)
with buc_pool.connect() as conn:
    conn.execute(text("SELECT 'OK' FROM dual"))
    print("BUC connection OK")

postgres_pool = sqlalchemy.create_engine(
    "postgresql+psycopg2://",
    creator=get_postgres_conn,
)
with postgres_pool.connect() as conn:
    conn.execute(text("SELECT 'OK'"))
    print("Postgres connection OK")

print("Year period")
year = int(input())
print("Month period")
month = int(input())

month_range = calendar.monthrange(year, month)
start_month = date(year, month, 1)
end_month = date(year, month, month_range[1])

print(f"Extracting period: from {start_month} to {end_month}")

with postgres_pool.begin() as postgres:
    with oracle_pool.begin() as ora:
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, 
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_AVOL
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_avol", params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE, 
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, 
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_BONO
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_bono_uni", params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, 
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_COMP
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_comret", params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, 
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_GOB
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_gobierno", params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION,
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_RCV
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_rcv", params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, 
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_SAR
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_sar", params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION,
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_VIV
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_viv", params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT DISTINCT sct.FCN_ID_TIPO_SUBCTA, sct.FCN_ID_REGIMEN, 
               sct.FCN_ID_CAT_SUBCTA, cat.FCC_VALOR
        FROM TCCRXGRAL_TIPO_SUBCTA sct
        INNER JOIN THCRXGRAL_CAT_CATALOGO cat 
                ON sct.FCN_ID_CAT_SUBCTA = cat.FCN_ID_CAT_CATALOGO
        """, "tipo_subcuentas", limit=100)
        extract_dataset(ora, postgres, """
        SELECT FCN_ID_SIEFORE,FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
        FROM TCAFOGRAL_VALOR_ACCION
        WHERE FCD_FEH_ACCION IN (
            SELECT MAX(FCD_FEH_ACCION) 
            FROM TCAFOGRAL_VALOR_ACCION
        )
        """, "valor_acciones", limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FTC_FOLIO, FCN_ID_SIEFORE, FTF_MONTO_ACCIONES, 
               FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_CMS
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_comisiones", params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_DIA_ACCIONES, FTC_NUM_CTA_INVDUAL, FCN_ID_SIEFORE,
               FCN_ID_TIPO_SUBCTA, FTD_FEH_LIQUIDACION 
        FROM THAFOGRAL_SALDO_HISTORICO
        WHERE FTN_DIA_ACCIONES = :end_day
        """, "saldo_inicial", params={"end_day": end_month.day}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_DIA_ACCIONES, FTC_NUM_CTA_INVDUAL, FCN_ID_SIEFORE,
               FCN_ID_TIPO_SUBCTA, FTD_FEH_LIQUIDACION 
        FROM THAFOGRAL_SALDO_HISTORICO
        WHERE FTN_DIA_ACCIONES = :start_day
        """, "saldo_final", params={"start_day": start_month.day}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_DIA_ACCIONES, FTC_NUM_CTA_INVDUAL, FCN_ID_SIEFORE,
               FCN_ID_TIPO_SUBCTA, FTD_FEH_LIQUIDACION
        FROM THAFOGRAL_SALDO_HISTORICO
        WHERE FTN_DIA_ACCIONES = (
            SELECT MAX(FTN_DIA_ACCIONES) 
            FROM THAFOGRAL_SALDO_HISTORICO
        )
        """, "fecha_acciones", limit=100)

    with buc_pool.begin() as buc:
        extract_dataset(buc, postgres, """
        SELECT C.NUMERO AS CUENTA,
               (PF.NOMBRE ||' '||PF.APELLIDOPATERNO ||' '||PF.APELIDOMATERNO) AS NOMBRE,
               PCR.IDPERSONA,
               PF.IDSEXO,
               NSS.VALOR_IDENTIFICADOR AS NSS,
               CURP.VALOR_IDENTIFICADOR AS CURP,
               RFC.VALOR_IDENTIFICADOR AS RFC,
               PF.FECHANACIMIENTO
        FROM CONTRATO C
           INNER JOIN PERSONA_CONT_ROL PCR ON (C.IDCONTRATO = PCR.IDCONTRATO)
           INNER JOIN PERSONA_FISICA PF ON (PCR.IDPERSONA = PF.IDPERSONA)
           LEFT JOIN IDENTIFICADOR NSS ON (PCR.IDPERSONA = NSS.IDPERSONA AND NSS.IDTIPOIDENTIFICADOR = 3 AND NSS.VALIDO = 1 AND NSS.ACTIVO = 1)
           LEFT JOIN IDENTIFICADOR CURP ON (PCR.IDPERSONA = CURP.IDPERSONA AND CURP.IDTIPOIDENTIFICADOR = 2 AND CURP.VALIDO = 1 AND CURP.ACTIVO = 1)
           LEFT JOIN IDENTIFICADOR RFC ON (PCR.IDPERSONA = RFC.IDPERSONA AND RFC.IDTIPOIDENTIFICADOR = 1 AND RFC.VALIDO = 1 AND RFC.ACTIVO = 1)
           INNER JOIN CATALOGO_GENERAL E ON (C.IDSTATUSCONTRATO = E.IDCATALOGOGRAL)
        -- WHERE PCR.IDROL = 787 /* Rol cliente */
        --   AND C.IDLINEANEGOCIO = 763 /* Linea de negocio */
        --   AND C.NUMERO < 7000000000 /* Solo Afiliados */
        --   AND E.CLAVENATURAL NOT IN ('21','22','24','51','90') 
        """, "datos_demograficos", limit=10000)
        extract_dataset(buc, postgres, """
        SELECT D.IDDIRECCION, D.PREFERENTE, DI.VALIDO, DI.CALLE,
           DI.NUMEROEXTERIOR, DI.NUMEROINTERIOR , CP.CODIGOPOSTAL,
           DI.IDASENTAMIENTO, ASE.NOMBRE AS ASENTAMIENTO, M.NOMBRE AS MUNICIPIO,
           C.NOMBRE AS CIUDAD, E.NOMBRE AS ESTADO, P.NOMBRE AS PAIS,
           EST.DESCRIPCION AS ESTATUS_DOM, D.FECHACREACION AS FECHACREACION_DOM,
           D.FECHAACTUALIZACION AS FECHAACTUALIZACION_DOM
        FROM PERSONA A
            INNER JOIN DOMICILIO D ON (A.IDPERSONA = D.IDPERSONA)
            INNER JOIN DIRECCION DI ON (D.IDDIRECCION = DI.IDDIRECCION)
            INNER JOIN CATALOGO_GENERAL EST ON (D.IDSTATUSDOM = EST.IDCATALOGOGRAL)
            LEFT JOIN CODIGO_POSTAL CP ON (DI.IDCODIGOPOSTAL = CP.IDCODIGOPOSTAL)
            LEFT JOIN ASENTAMIENTO ASE ON (DI.IDASENTAMIENTO = ASE.IDASENTAMIENTO)
            LEFT JOIN ESTADO E ON (CP.IDESTADO = E.IDESTADO)
            LEFT JOIN CIUDAD C ON (CP.IDCIUDAD = C.IDCIUDAD)
            LEFT JOIN MUNICIPIO M ON (CP.IDMUNICIPIO = M.IDMUNICIPIO)
            LEFT JOIN PAIS P ON (CP.IDPAIS = P.IDPAIS)
        -- WHERE D.IDTIPODOM = 818 -- Tipo de domicilio Particular
        --   AND D.IDSTATUSDOM = 761 -- ACTIVO
        --   AND D.PREFERENTE = 1 -- Domicilio preferente
        """, "datos_geograficos", limit=100)
