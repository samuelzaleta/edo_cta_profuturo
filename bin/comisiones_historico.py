from profuturo.extraction import  _get_spark_session, read_table_insert_temp_view, _write_spark_dataframe
from profuturo.common import define_extraction
from profuturo.database import get_postgres_pool,configure_postgres_spark_dev
from datetime import datetime as today
from pyspark.sql.functions import col, monotonically_increasing_id
import datetime
import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import extras
import time



postgres_pool = get_postgres_pool()
phase = 11
user = 0
area = 2


def last_day_of_month(any_day): 
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4) # this will never fail return next_month - datetime.timedelta(days=next_month.day)
    return next_month - datetime.timedelta(days=next_month.day)


with define_extraction(phase, area, postgres_pool, postgres_pool) as (postgres, _):
    print('SparkSession')
    spark = _get_spark_session()
    ##Obtener catalogos Postgres
    query_tipo_movimiento =  " SELECT \
                        tmp.\"FTN_ID_MOVIMIENTO_PROFUTURO\", \
                        tcs.\"FTN_ID_TIPO_SUBCTA\" \
                        FROM \"GESTOR\".\"TCGESPRO_MOVIMIENTO_PROFUTURO\" tmp \
                        inner join \"MAESTROS\".\"TCDATMAE_TIPO_SUBCUENTA\" tcs on tcs.\"FTN_ID_TIPO_SUBCTA\" = tmp.\"FCN_ID_SUBCUENTA_INTEGRITY\" \
                        WHERE tmp.\"FTN_ID_MOVIMIENTO_PROFUTURO\" IN(9910, 9911, 9912, 9913) "
    query_tipo_sbcta =  "SELECT * FROM \"MAESTROS\".\"TCDATMAE_TIPO_SUBCUENTA_INTEGRITY_VAR\" "

    read_table_insert_temp_view(
        configure_postgres_spark_dev,
        query_tipo_movimiento,
        "TIPOSMOVIMIENTOS",
    )

    read_table_insert_temp_view(
        configure_postgres_spark_dev,
        query_tipo_sbcta,
        "TIPOSBCTA",
    )

    listaSubctaDF = spark.sql("""SELECT 
                                 FTN_ID_RECORD as ftn_id,
                                 FCN_ID_TIPO_SUBCTA as id_tipo_sbcta,
                                 FCN_REGISTRO as fcc_registro,
                                 FCC_VAR_INTEGRITY as fcc_var_integrity
                                 FROM TIPOSBCTA""").toPandas()
    listaMovimientosDF= spark.sql("""SELECT 
                                    FTN_ID_MOVIMIENTO_PROFUTURO as FTN_ID_MOVIMIENTO_PROFUTURO,
                                    FTN_ID_TIPO_SUBCTA as FTN_ID_TIPO_SUBCTA
                                 FROM TIPOSMOVIMIENTOS""").toPandas()

    print(listaSieforeDF)
    print(listaSubctaDF)

    #medir tiempo ejecucion
    inicio = time.time()
    hoy = today.today()
    print(inicio, hoy)

    saldosDFfile = spark.read.text("gs://dataproc-spark-dev/COMISXSALDO_202203_CORTO.TXT")

    saldosDFfile2 = saldosDFfile.withColumn("cuenta", saldosDFfile["value"][0:10])\
                            .withColumn("periodo", saldosDFfile["value"][11:6])\
                            .withColumn("COM_RETIRO", saldosDFfile["value"][17:18])\
                            .withColumn("COM_CYVSOC", saldosDFfile["value"][36:18])\
                            .withColumn("COM_SAR_IM", saldosDFfile["value"][55:18])\
                            .withColumn("COM_SAR_IS", saldosDFfile["value"][74:18])\
                            .withColumn("COM_AVOL", saldosDFfile["value"][93:18])\
                            .withColumn("COM_ACR", saldosDFfile["value"][112:18])\
                            .withColumn("COM_ALP", saldosDFfile["value"][131:18])\
                            .withColumn("COM_RCV_IS", saldosDFfile["value"][150:18])\
                            .withColumn("COM_AHO_SOL", saldosDFfile["value"][169:18])
                            #.withColumn("cuenta", saldosDFfile["value"].cast(IntegerType()))
    print(saldosDFfile2.printSchema())
    #print(saldosDFfile2.select("COM_AVOL").collect())
    #print(saldosDFfile2.show())

    print('File Rows:')
    print(saldosDFfile2.count())
    data = []
    var:str = ''
    mes:int = 0
    anio:int = 0
    fecha_liquida:datetime.date = None
    id_movimiento:int = None
    c:int= 0
    cuenta:str = None
    idSubCta:int = None
    idmov:int = None
    v_historico:str = 'HISTORICO'
    try:

        for df in saldosDFfile2.collect():
            anio = int(df['periodo'][0:4])
            mes = int(df['periodo'][4:6])
            fecha_liquida = last_day_of_month(datetime.date(anio, mes, 1))
            #id_resuSiefore = resuSiefore['fcn_id_siefore'].values
            cuenta = df['cuenta']

            if c == 0:
                data.append((1,1,1,1,1,1,1,0,0.0,fecha_liquida, hoy, v_historico, 1))
            
            
            #Busqueda de subcuenta
            if float(df['COM_RETIRO']) > 0:
                var = 'COM_RETIRO'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcn_code_var_comision']==var)]
                #idSubCta = resuSbcta['id_tipo_sbcta'].values
                #resMov = listaMovimientosDF[(listaMovimientosDF['FTN_ID_TIPO_SUBCTA'] == int(idSubCta[0]))]
                #idmov = resMov['FTN_ID_MOVIMIENTO_PROFUTURO'].values
                #data.append((int(df['cuenta']), int(df['periodo']), int(idmov[0]), None, 9486, None, None, 0, float(df[var]), fecha_liquida, hoy, v_historico, int(idSubCta[0])))
                #c+=1

            if float(df['COM_CYVSOC']) > 0:
                var = 'COM_CYVSOC'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcn_code_var_comision']==var)]
                idSubCta = resuSbcta['id_tipo_sbcta'].values
                resMov = listaMovimientosDF[(listaMovimientosDF['FTN_ID_TIPO_SUBCTA'] == int(idSubCta[0]))]
                #idmov = resMov['FTN_ID_MOVIMIENTO_PROFUTURO'].values
                #data.append((int(df['cuenta']), int(df['periodo']), int(idmov[0]), None, 9486, None, None, 0, float(df[var]), fecha_liquida, hoy, v_historico, int(idSubCta[0])))
                #c+=1

            #COM_SAR_IM
            if float(df['COM_SAR_IM']) > 0:
                var = 'COM_SAR_IM'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcn_code_var_comision']==var)]
                idSubCta = resuSbcta['id_tipo_sbcta'].values
                resMov = listaMovimientosDF[(listaMovimientosDF['FTN_ID_TIPO_SUBCTA'] == int(idSubCta[0]))]
                idmov = resMov['FTN_ID_MOVIMIENTO_PROFUTURO'].values
                data.append((int(df['cuenta']), int(df['periodo']), int(idmov[0]), None, 9486, None, None, 0, float(df[var]), fecha_liquida, hoy, v_historico, int(idSubCta[0])))
                c+=1

            #COM_SAR_IS
            if float(df['COM_SAR_IS']) > 0:
                var = 'COM_SAR_IS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcn_code_var_comision']==var)]
                idSubCta = resuSbcta['id_tipo_sbcta'].values
                #resMov = listaMovimientosDF[(listaMovimientosDF['FTN_ID_TIPO_SUBCTA'] == int(idSubCta[0]))]
                #idmov = resMov['FTN_ID_MOVIMIENTO_PROFUTURO'].values
                #data.append((int(df['cuenta']), int(df['periodo']), int(idmov[0]), None, 9486, None, None, 0, float(df[var]), fecha_liquida, hoy, v_historico, int(idSubCta[0])))
                #c+=1
            
            if float(df['COM_AVOL']) > 0:
                var = 'COM_AVOL'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcn_code_var_comision']==var)]
                idSubCta = resuSbcta['id_tipo_sbcta'].values
                resMov = listaMovimientosDF[(listaMovimientosDF['FTN_ID_TIPO_SUBCTA'] == int(idSubCta[0]))]
                idmov = resMov['FTN_ID_MOVIMIENTO_PROFUTURO'].values
                data.append((int(df['cuenta']), int(df['periodo']), int(idmov[0]), None, 9486, None, None, 0, float(df[var]), fecha_liquida, hoy, v_historico, int(idSubCta[0])))
                c+=1
            
            if float(df['COM_ACR']) > 0:
                var = 'COM_ACR'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcn_code_var_comision']==var)]
                idSubCta = resuSbcta['id_tipo_sbcta'].values
                resMov = listaMovimientosDF[(listaMovimientosDF['FTN_ID_TIPO_SUBCTA'] == int(idSubCta[0]))]
                idmov = resMov['FTN_ID_MOVIMIENTO_PROFUTURO'].values
                data.append((int(df['cuenta']), int(df['periodo']), int(idmov[0]), None, 9486, None, None, 0, float(df[var]), fecha_liquida, hoy, v_historico, int(idSubCta[0])))
                c+=1

            #COM_ALP
            if float(df['COM_ALP']) > 0:
                var = 'COM_ALP'    
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcn_code_var_comision']==var)]
                idSubCta = resuSbcta['id_tipo_sbcta'].values
                #resMov = listaMovimientosDF[(listaMovimientosDF['FTN_ID_TIPO_SUBCTA'] == int(idSubCta[0]))]
                #idmov = resMov['FTN_ID_MOVIMIENTO_PROFUTURO'].values
                #data.append((int(df['cuenta']), int(df['periodo']), int(idmov[0]), None, 9486, None, None, 0, float(df[var]), fecha_liquida, hoy, v_historico, int(idSubCta[0])))
                #c+=1
            
            if float(df['COM_RCV_IS']) > 0:
                var = 'COM_RCV_IS'          
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcn_code_var_comision']==var)]
                idSubCta = resuSbcta['id_tipo_sbcta'].values
                resMov = listaMovimientosDF[(listaMovimientosDF['FTN_ID_TIPO_SUBCTA'] == int(idSubCta[0]))]
                idmov = resMov['FTN_ID_MOVIMIENTO_PROFUTURO'].values
                data.append((int(df['cuenta']), int(df['periodo']), int(idmov[0]), None, 9486, None, None, 0, float(df[var]), fecha_liquida, hoy, v_historico, int(idSubCta[0])))
                c+=1
            
            if float(df['COM_AHO_SOL']) > 0:
                var = 'COM_AHO_SOL'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcn_code_var_comision']==var)]
                idSubCta = resuSbcta['id_tipo_sbcta'].values
                # resMov = listaMovimientosDF[(listaMovimientosDF['FTN_ID_TIPO_SUBCTA'] == int(idSubCta[0]))]
                # idmov = resMov['FTN_ID_MOVIMIENTO_PROFUTURO'].values
                # data.append((int(df['cuenta']), int(df['periodo']), int(idmov[0]), None, 9486, None, None, 0, float(df[var]), fecha_liquida, hoy, v_historico, int(idSubCta[0])))
                # c+=1
                

    
    except Exception as error:
        print('error: ')
        print(error)
        print(var)
        print('registro ' +str(c))
        print(cuenta)

    print('Rows to insert:')
    print(c)

    #df to insert into "comisiones"
    # Define the column names
    columns_insert = ["FCN_CUENTA", "FCN_ID_PERIODO", "FCN_ID_CONCEPTO_MOVIMIENTO", "FCN_ID_MOVIMIENTO", "FCN_ID_TIPO_MOVIMIENTO", "FCN_ID_SIEFORE",
            "FCN_FOLIO", "FTF_MONTO_ACCIONES", "FTF_MONTO_PESOS", "FTD_FEH_LIQUIDACION", "FTD_FECHA_INGESTA", "FTC_EXTRACTOR_INGESTA",
            "FTN_TIPO_SUBCTA"]


    df_insert = spark.createDataFrame(data, columns_insert)
    df_insert = df_insert.withColumn("row_id", monotonically_increasing_id())
    df_insert = df_insert.filter(df_insert.row_id != 0)

    _write_spark_dataframe(df_insert, postgres_pool, "HECHOS"."TTHECHOS_COMISION")

    fin = time.time()
    print("Execution time")
    print(fin - inicio)