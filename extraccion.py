from sqlalchemy.engine.base import Connection
from sqlalchemy import text
from typing import Any, Dict
from datetime import datetime, date
import oracledb
import psycopg2
import pandas as pd
import sqlalchemy
import calendar


class MemoryLogger:
    __log: str

    def log(self, message: str) -> None:
        self.__log = self.__log + message + '\n'

    def get_log(self) -> str:
        return self.__log


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
        database="postgres",
        options="-c search_path=public",
    )


def get_masters_conn() -> psycopg2.Connection:
    return psycopg2.connect(
        host="34.72.193.129",
        user="alexo",
        password="Alexo123",
        database="postgres",
        options="-c search_path=maestros",
    )


def count_rows(conn: Connection, table: str) -> int:
    return conn.scalar(text(f"""
    SELECT COUNT(*) AS count
    FROM {table}
    """))


def write_binnacle(conn: Connection, term: int, phase: int, start: datetime, end: datetime) -> None:
    cursor = conn.execute(text("""
    SELECT amarillo, rojo
    FROM tcgespro_fase
    WHERE id_fase = :phase
    """), {'phase': phase})

    if cursor.rowcount != 1:
        print(f'Phase {phase} not found')
        return

    delta = end - start
    service_level = cursor.fetchone()

    flag: str
    if delta <= service_level[0]:
        flag = "Verde"
    elif delta <= service_level[1]:
        flag = "Amarillo"
    else:
        flag = "Rojo"

    conn.execute(text("""
    INSERT INTO ttgespro_bitacora_estado_cuenta (fecha_hora_inicio, fecha_hora_final, bandera_nivel_servicio, id_formato_estado_cuenta, id_periodo, id_fase) 
    VALUES (:start, :end, :flag, :format, :term, :phase)
    """), {
        "start": start,
        "end": end,
        "flag": flag,
        "format": 0,
        "term": term,
        "phase": phase,
    })


def extract_simple_indicator(conn: Connection, table: str, column: str) -> float:
    return round(conn.scalar(text(f"""
    SELECT SUM({column})
    FROM {table}
    """)), 2)


def extract_dataset(
    origin: Connection,
    destination: Connection,
    query: str,
    table: str,
    term: int,
    phase: int,
    params: Dict[str, Any] = None,
    indicator: str = None,
    limit: int = None,
):
    if params is None:
        params = {}
    if limit is not None:
        query = f"SELECT * FROM ({query}) WHERE ROWNUM <= :limit"
        params["limit"] = limit

    print(f"Extracting {table}...")

    start = datetime.now()
    df_pd = pd.read_sql_query(text(query), origin, params=params)
    df_pd.to_sql(table, destination, if_exists="replace")

    df_sp = spark.createDataFrame(df_pd)
    df_sp.write.parquet(f"gs://spark-dataproc-poc/Datalake/{table}", mode="overwrite")
    end = datetime.now()

    rows = count_rows(destination, table)
    app_log.log(f"Extracted {rows} rows from '{table}'")

    if indicator is not None:
        statistics = extract_simple_indicator(destination, table, indicator)
        app_log.log(f"Indicator {indicator} have a value of {statistics} from '{table}'")

    write_binnacle(destination, term, phase, start, end)

    print(f"Done extracting {table}!")


app_log = MemoryLogger()
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

masters_pool = sqlalchemy.create_engine(
    "postgresql+psycopg2://",
    creator=get_masters_conn,
)
with masters_pool.connect() as conn:
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
        SELECT DISTINCT sct.FCN_ID_TIPO_SUBCTA, sct.FCN_ID_REGIMEN, 
               sct.FCN_ID_CAT_SUBCTA, cat.FCC_VALOR
        FROM TCCRXGRAL_TIPO_SUBCTA sct
        INNER JOIN THCRXGRAL_CAT_CATALOGO cat 
                ON sct.FCN_ID_CAT_SUBCTA = cat.FCN_ID_CAT_CATALOGO
        """, "tipo_subcuentas", 1, phase=1, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, 
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_AVOL
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_avol", 1, phase=1, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE, 
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, 
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_BONO
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_bono", 1, phase=1, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, 
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_COMP
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_comret", 1, phase=1, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, 
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_GOB
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_gobierno", 1, phase=1, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION,
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_RCV
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_rcv", 1, phase=1, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, 
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_SAR
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_sar", 1, phase=1, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FCN_ID_TIPO_SUBCTA, FTC_FOLIO, FCN_ID_SIEFORE,
               FTF_MONTO_ACCIONES, FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION,
               FCN_ID_TIPO_MOV, FCN_ID_CONCEPTO_MOV, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_VIV
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_viv", 1, phase=1, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_ID_MOV, FTC_FOLIO, FCN_ID_SIEFORE, FTF_MONTO_ACCIONES, 
               FTN_NUM_CTA_INVDUAL, FTD_FEH_LIQUIDACION, FTF_MONTO_PESOS
        FROM TTAFOGRAL_MOV_CMS
        WHERE FTD_FEH_LIQUIDACION BETWEEN :start AND :end
        """, "mov_comisiones", 1, phase=2, params={"start": start_month, "end": end_month}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FCN_ID_SIEFORE,FCN_ID_REGIMEN, FCN_VALOR_ACCION, FCD_FEH_ACCION
        FROM TCAFOGRAL_VALOR_ACCION
        WHERE FCD_FEH_ACCION IN (
            SELECT MAX(FCD_FEH_ACCION) 
            FROM TCAFOGRAL_VALOR_ACCION
        )
        """, "valor_acciones", 1, phase=3, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_DIA_ACCIONES, FTC_NUM_CTA_INVDUAL, FCN_ID_SIEFORE,
               FCN_ID_TIPO_SUBCTA, FTD_FEH_LIQUIDACION
        FROM THAFOGRAL_SALDO_HISTORICO
        WHERE FTN_DIA_ACCIONES = (
            SELECT MAX(FTN_DIA_ACCIONES) 
            FROM THAFOGRAL_SALDO_HISTORICO
        )
        """, "fecha_acciones", 1, phase=3, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_DIA_ACCIONES, FTC_NUM_CTA_INVDUAL, FCN_ID_SIEFORE,
               FCN_ID_TIPO_SUBCTA, FTD_FEH_LIQUIDACION 
        FROM THAFOGRAL_SALDO_HISTORICO
        WHERE FTN_DIA_ACCIONES = :end_day
        """, "saldo_inicial", 1, phase=4, params={"end_day": end_month.day}, limit=100)
        extract_dataset(ora, postgres, """
        SELECT FTN_DIA_ACCIONES, FTC_NUM_CTA_INVDUAL, FCN_ID_SIEFORE,
               FCN_ID_TIPO_SUBCTA, FTD_FEH_LIQUIDACION 
        FROM THAFOGRAL_SALDO_HISTORICO
        WHERE FTN_DIA_ACCIONES = :start_day
        """, "saldo_final", 1, phase=5, params={"start_day": start_month.day}, limit=100)

with masters_pool.begin() as masters:
    with buc_pool.begin() as buc:
        extract_dataset(buc, masters, """
        SELECT C.NUMERO AS CUENTA, DP.CORREOS,
               (PF.NOMBRE || ' ' || PF.APELLIDOPATERNO || ' ' ||PF.APELIDOMATERNO) AS NOMBRE,
               DI.CALLE, DI.NUMEROEXTERIOR AS NUMERO, ASE.NOMBRE AS COLONIA,
               C.NOMBRE AS DELEGACION, CP.CODIGOPOSTAL, E.NOMBRE AS ENTIDAD_FEDERATIVA,
               NSS.VALOR_IDENTIFICADOR AS NSS,
               CURP.VALOR_IDENTIFICADOR AS CURP, 
               RFC.VALOR_IDENTIFICADOR AS RFC
        FROM CONTRATO C
            INNER JOIN PERSONA_CONT_ROL PCR ON (C.IDCONTRATO = PCR.IDCONTRATO)
            INNER JOIN PERSONA P ON (PCR.IDPERSONA = P.IDPERSONA)
            INNER JOIN PERSONA_ORIGEN PO ON (P.IDPERSONA = PO.IDPERSONA)
            INNER JOIN DO_PERSONA DP ON (PO.IDPERSONAORIGEN = DP.IDPERSONAORIGEN)
            INNER JOIN PERSONA_FISICA PF ON (P.IDPERSONA = PF.IDPERSONA)
            LEFT JOIN IDENTIFICADOR NSS ON (PF.IDPERSONA = NSS.IDPERSONA AND NSS.IDTIPOIDENTIFICADOR = 3 AND NSS.VALIDO = 1 AND NSS.ACTIVO = 1)
            LEFT JOIN IDENTIFICADOR CURP ON (PF.IDPERSONA = CURP.IDPERSONA AND CURP.IDTIPOIDENTIFICADOR = 2 AND CURP.VALIDO = 1 AND CURP.ACTIVO = 1)
            LEFT JOIN IDENTIFICADOR RFC ON (PF.IDPERSONA = RFC.IDPERSONA AND RFC.IDTIPOIDENTIFICADOR = 1 AND RFC.VALIDO = 1 AND RFC.ACTIVO = 1)
            INNER JOIN DOMICILIO D ON (PF.IDPERSONA = D.IDPERSONA)
            INNER JOIN DIRECCION DI ON (D.IDDIRECCION = DI.IDDIRECCION)
            LEFT JOIN CODIGO_POSTAL CP ON (DI.IDCODIGOPOSTAL = CP.IDCODIGOPOSTAL)
            LEFT JOIN ESTADO E ON (CP.IDESTADO = E.IDESTADO)
            LEFT JOIN MUNICIPIO M ON (CP.IDMUNICIPIO = M.IDMUNICIPIO)
            LEFT JOIN CIUDAD C ON (CP.IDCIUDAD = C.IDCIUDAD)
            LEFT JOIN ASENTAMIENTO ASE ON (DI.IDASENTAMIENTO = ASE.IDASENTAMIENTO)
        -- WHERE PCR.IDROL = 787 -- Rol cliente
        --   AND C.IDLINEANEGOCIO = 763 -- Linea de negocio
        --   AND C.NUMERO < 7000000000 -- Solo Afiliados
        --   AND E.CLAVENATURAL NOT IN ('21','22','24','51','90')
        --   AND D.IDTIPODOM = 818 /Tipo de domicilio Particular/
        --   AND D.IDSTATUSDOM = 761 -- ACTIVO
        --   AND D.PREFERENTE = 1 --Domicilio preferente
        """, "datos_geograficos", 1, phase=6, limit=100)
