from profuturo.extraction import  _get_spark_session, read_table_insert_temp_view, _write_spark_dataframe
from profuturo.common import define_extraction
from profuturo.database import get_postgres_pool,configure_postgres_spark_dev
from datetime import datetime as today
from pyspark.sql.functions import col, monotonically_increasing_id, regexp_replace
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
    query_tipo_siefore =  "SELECT * FROM \"MAESTROS\".\"TCDATMAE_SIEFORE_INTEGRITY\" "
    query_tipo_sbcta =  "SELECT * FROM \"MAESTROS\".\"TCDATMAE_TIPO_SUBCUENTA_INTEGRITY_VAR\" "

    read_table_insert_temp_view(
        configure_postgres_spark_dev,
        query_tipo_siefore,
        "TIPOSIEROFE",
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
    listaSieforeDF= spark.sql("""SELECT 
                                    FTN_ID_RECORD as ftn_id,
                                    FCN_ID_SIEFORE as fcn_id_siefore,
                                    FCN_CODE_SIEFORE_INTEGRITY as fcn_code_integrity
                                 FROM TIPOSIEROFE""").toPandas()

    print(listaSieforeDF)
    print(listaSubctaDF)
    #print(listaSieforeDF.values)




    #medir tiempo ejecucion
    inicio = time.time()
    hoy = today.today()
    print(inicio, hoy)

    saldosDFfile = spark.read.text("gs://dataproc-spark-dev/SALDOSTDF_202203.TXT")

    saldosDFfile.show(2)

    print(type(saldosDFfile))
    data = []
    saldosDFfile2 = saldosDFfile.withColumn("periodo", saldosDFfile["value"][0:6]) \
                .withColumn("cuenta", saldosDFfile["value"][7:10]) \
                .withColumn("service", saldosDFfile["value"][17:1]) \
                .withColumn("SAL_SALD_SIEFORE", saldosDFfile["value"][18:2]) \
                .withColumn("SAL_SALD_RETS", saldosDFfile["value"][20:19]) \
                .withColumn("SAL_SALD_RET8S", saldosDFfile["value"][40:19]) \
                .withColumn("SAL-SALD-CYVS", saldosDFfile["value"][60:19]) \
                .withColumn("SAL-SALD-CYVTS", saldosDFfile["value"][80:19]) \
                .withColumn("SAL-SALD-CSOS", saldosDFfile["value"][100:19]) \
                .withColumn("SAL-SALD-ESTS", saldosDFfile["value"][120:19]) \
                .withColumn("SAL-SALD-ESPS", saldosDFfile["value"][140:19]) \
                .withColumn("SAL-SALD-CRES", saldosDFfile["value"][160:19]) \
                .withColumn("SAL-SALD-CREDS", saldosDFfile["value"][180:19]) \
                .withColumn("SAL-SALD-SARS", saldosDFfile["value"][200:19]) \
                .withColumn("SAL-SALD-AVDS", saldosDFfile["value"][220:19]) \
                .withColumn("SAL-SALD-AVPS", saldosDFfile["value"][240:19]) \
                .withColumn("SAL-SALD-AVES", saldosDFfile["value"][260:19]) \
                .withColumn("SAL-SALD-ALPS", saldosDFfile["value"][280:19]) \
                .withColumn("SAL-SALD-ALPDS", saldosDFfile["value"][300:19]) \
                .withColumn("SAL-SALD-ALPES", saldosDFfile["value"][320:19]) \
                .withColumn("SAL-SALD-SDOV92", saldosDFfile["value"][340:19]) \
                .withColumn("SAL-SALD-SDOV97", saldosDFfile["value"][360:19]) \
                .withColumn("SAL-SALD-SDOBUD", saldosDFfile["value"][380:19]) \
                .withColumn("SAL_SALD_RETS_PESOS", saldosDFfile["value"][400:19]) \
                .withColumn("SAL_SALD_RET8S_PESOS", saldosDFfile["value"][420:19]) \
                .withColumn("SAL-SALD-CYVS_PESOS", saldosDFfile["value"][440:19]) \
                .withColumn("SAL-SALD-CYVTS_PESOS", saldosDFfile["value"][460:19]) \
                .withColumn("SAL-SALD-CSOS_PESOS", saldosDFfile["value"][480:19]) \
                .withColumn("SAL-SALD-ESTS_PESOS", saldosDFfile["value"][500:19]) \
                .withColumn("SAL-SALD-ESPS_PESOS", saldosDFfile["value"][520:19]) \
                .withColumn("SAL-SALD-CRES_PESOS", saldosDFfile["value"][540:19]) \
                .withColumn("SAL-SALD-CREDS_PESOS", saldosDFfile["value"][560:19]) \
                .withColumn("SAL-SALD-SARS_PESOS", saldosDFfile["value"][580:19]) \
                .withColumn("SAL-SALD-AVDS_PESOS", saldosDFfile["value"][600:19]) \
                .withColumn("SAL-SALD-AVPS_PESOS", saldosDFfile["value"][620:19]) \
                .withColumn("SAL-SALD-AVES_PESOS", saldosDFfile["value"][640:19]) \
                .withColumn("SAL-SALD-ALPS_PESOS", saldosDFfile["value"][660:19]) \
                .withColumn("SAL-SALD-ALPDS_PESOS", saldosDFfile["value"][680:19]) \
                .withColumn("SAL-SALD-ALPES_PESOS", saldosDFfile["value"][700:19]) \
                .withColumn("SAL-SALD-SDOV92_PESOS", saldosDFfile["value"][720:19]) \
                .withColumn("SAL-SALD-SDOV97_PESOS", saldosDFfile["value"][740:19]) \
                .withColumn("SAL-SALD-SDOBUD_PESOS", saldosDFfile["value"][760:19]) \
                .withColumn("TIPO_CAMBIO", saldosDFfile["value"][780:4])

    saldosDFfile2 = saldosDFfile2.withColumn("SAL_SALD_RETS", regexp_replace("SAL_SALD_RETS", ' ', ''))\
                             .withColumn("SAL_SALD_RET8S", regexp_replace("SAL_SALD_RET8S", ' ', ''))\
                             .withColumn("SAL-SALD-CYVS", regexp_replace("SAL-SALD-CYVS", ' ', ''))\
                             .withColumn("SAL-SALD-CYVTS", regexp_replace("SAL-SALD-CYVTS", ' ', ''))\
                             .withColumn("SAL-SALD-CSOS", regexp_replace("SAL-SALD-CSOS", ' ', ''))\
                             .withColumn("SAL-SALD-ESTS",regexp_replace("SAL-SALD-ESTS", ' ', ''))\
                             .withColumn("SAL-SALD-ESPS", regexp_replace("SAL-SALD-ESPS", ' ', ''))\
                             .withColumn("SAL-SALD-CRES", regexp_replace("SAL-SALD-CRES", ' ', ''))\
                             .withColumn("SAL-SALD-CREDS", regexp_replace("SAL-SALD-CREDS", ' ', ''))\
                             .withColumn("SAL-SALD-SARS", regexp_replace("SAL-SALD-SARS", ' ', ''))\
                             .withColumn("SAL-SALD-AVDS", regexp_replace("SAL-SALD-AVDS", ' ', ''))\
                             .withColumn("SAL-SALD-AVPS", regexp_replace("SAL-SALD-AVPS", ' ', ''))\
                             .withColumn("SAL-SALD-AVES", regexp_replace("SAL-SALD-AVES", ' ', ''))\
                             .withColumn("SAL-SALD-ALPS", regexp_replace("SAL-SALD-ALPS", ' ', ''))\
                             .withColumn("SAL-SALD-ALPDS", regexp_replace("SAL-SALD-ALPDS", ' ', ''))\
                             .withColumn("SAL-SALD-ALPES", regexp_replace("SAL-SALD-ALPES", ' ', ''))\
                             .withColumn("SAL-SALD-SDOV92", regexp_replace("SAL-SALD-SDOV92", ' ', ''))\
                             .withColumn("SAL-SALD-SDOV97", regexp_replace("SAL-SALD-SDOV97", ' ', ''))\
                             .withColumn("SAL-SALD-SDOBUD", regexp_replace("SAL-SALD-SDOBUD", ' ', ''))\
                             .withColumn("SAL_SALD_RETS_PESOS", regexp_replace("SAL_SALD_RETS_PESOS", ' ', ''))\
                             .withColumn("SAL_SALD_RET8S_PESOS", regexp_replace("SAL_SALD_RET8S_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-CYVS_PESOS", regexp_replace("SAL-SALD-CYVS_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-CYVTS_PESOS", regexp_replace("SAL-SALD-CYVTS_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-CSOS_PESOS", regexp_replace("SAL-SALD-CSOS_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-ESTS_PESOS", regexp_replace("SAL-SALD-ESTS_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-ESPS_PESOS", regexp_replace("SAL-SALD-ESPS_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-CRES_PESOS", regexp_replace("SAL-SALD-CRES_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-CREDS_PESOS", regexp_replace("SAL-SALD-CREDS_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-SARS_PESOS", regexp_replace("SAL-SALD-SARS_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-AVDS_PESOS", regexp_replace("SAL-SALD-AVDS_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-AVPS_PESOS", regexp_replace("SAL-SALD-AVPS_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-AVES_PESOS", regexp_replace("SAL-SALD-AVES_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-ALPS_PESOS", regexp_replace("SAL-SALD-ALPS_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-ALPDS_PESOS", regexp_replace("SAL-SALD-ALPDS_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-ALPES_PESOS", regexp_replace("SAL-SALD-ALPES_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-SDOV92_PESOS", regexp_replace("SAL-SALD-SDOV92_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-SDOV97_PESOS", regexp_replace("SAL-SALD-SDOV97_PESOS", ' ', ''))\
                             .withColumn("SAL-SALD-SDOBUD_PESOS", regexp_replace("SAL-SALD-SDOBUD_PESOS", ' ', ''))

    #.withColumn("cuenta", saldosDFfile["value"].cast(IntegerType()))

    #print(saldosDFfile2.printSchema())
    saldosDFfile2.show(2)
    df =saldosDFfile2.select("SAL_SALD_RETS_PESOS").show()

    saldosDFfile2.printSchema()

    var:str = ''
    mes:int = 0
    anio:int = 0
    fecha_liquida:datetime.date = None
    feh_accion:datetime.datetime = None
    c:int = 0
    print('File Rows:')
    print("conteo",saldosDFfile2.count())
    cuenta:str = None
    v_historico:str = 'HISTORICO'

    try:

        for df in saldosDFfile2.collect():
            print()
            anio = int(df['periodo'][0:4])
            mes = int(df['periodo'][4:6])
            fecha_liquida = last_day_of_month(datetime.date(anio, mes, 1))
            feh_accion = datetime.datetime.strptime(str(fecha_liquida), '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S.%f')
            resuSiefore = listaSieforeDF[listaSieforeDF['fcn_code_integrity'].astype(int) == int(df['SAL_SALD_SIEFORE'])]
            id_resuSiefore = resuSiefore['fcn_id_siefore'].values

            # Busqueda de subcuenta
            if float(df['SAL_SALD_RETS']) > 0 or float(df['SAL_SALD_RETS']) < 0:
                var = 'SAL-SALD-RETS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values

                data.append((int(df['cuenta']), int(df['periodo']), df['SAL_SALD_RETS'], int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', df['SAL_SALD_RETS_PESOS'], hoy,
                             v_historico))
                print(data)
                c += 1

            if float(str(df['SAL_SALD_RET8S']).replace(' ', '')) > 0 or float(
                    str(df['SAL_SALD_RET8S']).replace(' ', '')) < 0:
                var = 'SAL-SALD-RET8S'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(str(df['SAL_SALD_RET8S']).replace(' ', '')),
                             int(id_resuSiefore[0]), int(id_postgres[0]), fecha_liquida, feh_accion, 'F',
                             float(str(df['SAL_SALD_RET8S_PESOS']).replace(' ', '')), hoy, v_historico))
                c += 1

            if float(df['SAL-SALD-CYVS']) > 0 or float(df['SAL-SALD-CYVS']) < 0:
                var = 'SAL-SALD-CYVS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-CYVS']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-CYVS_PESOS']), hoy,
                             v_historico))
                c += 1

            if float(df['SAL-SALD-CYVTS']) > 0 or float(df['SAL-SALD-CYVTS']) < 0:
                var = 'SAL-SALD-CYVTS'
                # r = ()
                # i = 0
                # r, c = busca_subcuenta(var, df, listaSubctaDF, id_resuSiefore, fecha_liquida, feh_accion, hoy, v_historico)
                # data.append(r)
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-CYVTS']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-CYVTS_PESOS']),
                             hoy, v_historico))
                c += 1

            if float(df['SAL-SALD-CSOS']) > 0 or float(df['SAL-SALD-CSOS']) < 0:
                var = 'SAL-SALD-CSOS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-CSOS']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-CSOS_PESOS']), hoy,
                             v_historico))
                c += 1

            if float(df['SAL-SALD-ESTS']) > 0 or float(df['SAL-SALD-ESTS']) < 0:
                var = 'SAL-SALD-ESTS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-ESTS']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-ESTS_PESOS']), hoy,
                             v_historico))
                c += 1

            if float(df['SAL-SALD-ESPS']) > 0 or float(df['SAL-SALD-ESPS']) < 0:
                var = 'SAL-SALD-ESPS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-ESPS']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-ESPS_PESOS']), hoy,
                             v_historico))
                c += 1

            if float(df['SAL-SALD-CRES']) > 0 or float(df['SAL-SALD-CRES']) < 0:
                var = 'SAL-SALD-CRES'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-CRES']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-CRES_PESOS']), hoy,
                             v_historico))
                c += 1

            if float(df['SAL-SALD-CREDS']) > 0 or float(df['SAL-SALD-CREDS']) < 0:
                var = 'SAL-SALD-CREDS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                if id_resuSiefore:
                    data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-CREDS']),
                                 int(id_resuSiefore[0]), int(id_postgres[0]), fecha_liquida, feh_accion, 'F',
                                 float(df['SAL-SALD-CREDS_PESOS']), hoy, v_historico))
                    c += 1
                else:
                    print('no subcta' + cuenta + ' - serv' + str(df['service']))

            if float(df['SAL-SALD-SARS']) > 0 or float(df['SAL-SALD-SARS']) < 0:
                var = 'SAL-SALD-SARS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-SARS']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-SARS_PESOS']), hoy,
                             v_historico))
                c += 1

            if float(df['SAL-SALD-AVDS']) > 0 or float(df['SAL-SALD-AVDS']) < 0:
                var = 'SAL-SALD-AVDS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-AVDS']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-AVDS_PESOS']), hoy,
                             v_historico))
                c += 1

            if float(df['SAL-SALD-AVPS']) > 0 or float(df['SAL-SALD-AVPS']) < 0:
                var = 'SAL-SALD-AVPS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-AVPS']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-AVPS_PESOS']), hoy,
                             v_historico))
                c += 1

            if float(df['SAL-SALD-AVES']) > 0 or float(df['SAL-SALD-AVES']) < 0:
                var = 'SAL-SALD-AVES'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                # data.append((int(df['cuenta']),int(df['periodo']),float(df['SAL-SALD-AVES']),int(id_resuSiefore[0]),int(id_postgres[0]),fecha_liquida,feh_accion,'F',float(df['SAL-SALD-AVES']),hoy,None))
                c += 1

            if float(df['SAL-SALD-ALPS']) > 0 or float(df['SAL-SALD-ALPS']) < 0:
                var = 'SAL-SALD-ALPS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-ALPS']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-ALPS_PESOS']), hoy,
                             v_historico))
                c += 1

            if float(df['SAL-SALD-ALPDS']) > 0 or float(df['SAL-SALD-ALPDS']) < 0:
                var = 'SAL-SALD-ALPDS'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-ALPDS']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-ALPDS_PESOS']),
                             hoy, v_historico))
                c += 1

            if float(df['SAL-SALD-ALPES']) > 0 or float(df['SAL-SALD-ALPES']) < 0:
                var = 'SAL-SALD-ALPES'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-ALPES']), int(id_resuSiefore[0]),
                             int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-ALPES_PESOS']),
                             hoy, v_historico))
                c += 1

            if float(df['SAL-SALD-SDOV92']) > 0 or float(df['SAL-SALD-SDOV92']) < 0:
                var = 'SAL-SALD-SDOV92'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((
                            int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-SDOV92']), int(id_resuSiefore[0]),
                            int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-SDOV92_PESOS']),
                            hoy, v_historico))
                c += 1

            if float(df['SAL-SALD-SDOV97']) > 0 or float(df['SAL-SALD-SDOV97']) < 0:
                var = 'SAL-SALD-SDOV97'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((
                            int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-SDOV97']), int(id_resuSiefore[0]),
                            int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-SDOV97_PESOS']),
                            hoy, v_historico))
                c += 1

            if float(df['SAL-SALD-SDOBUD']) > 0 or float(df['SAL-SALD-SDOBUD']) < 0:
                var = 'SAL-SALD-SDOBUD'
                resuSbcta = listaSubctaDF[(listaSubctaDF['fcc_var_integrity'] == var) & (
                            listaSubctaDF['fcc_registro'].astype(int) == int(df['service']))]
                id_postgres = resuSbcta['id_tipo_sbcta'].values
                data.append((
                            int(df['cuenta']), int(df['periodo']), float(df['SAL-SALD-SDOBUD']), int(id_resuSiefore[0]),
                            int(id_postgres[0]), fecha_liquida, feh_accion, 'F', float(df['SAL-SALD-SDOBUD_PESOS']),
                            hoy, v_historico))
                c += 1

    except Exception as error:
        print('error: ' + var)
        print(error)
        print('registro ' + str(c))
        print(cuenta)

    
    columns_insert = ["FCN_CUENTA", 
                  "FCN_ID_PERIODO", 
                  "FTF_DIA_ACCIONES", 
                  "FCN_ID_SIEFORE",
                  "FCN_ID_TIPO_SUBCTA", 
                  "FTD_FEH_LIQUIDACION",
                  "FCD_FEH_ACCION", 
                  "FTC_TIPO_SALDO", 
                  "FTF_SALDO_DIA", 
                  "FTD_FECHA_INGESTA", 
                  "FTC_EXTRACTOR_INGESTA"]

    df_insert = spark.createDataFrame(data, columns_insert)
    df_insert = df_insert.withColumn("row_id", monotonically_increasing_id())
    df_insert = df_insert.filter(df_insert.row_id != 0)
    df_insert = df_insert.withColumn("FCD_FEH_ACCION", col("FCD_FEH_ACCION").cast(DateType()))

    _write_spark_dataframe(df_insert, postgres_pool, "HECHOS"."THHECHOS_SALDO_HISTORICO")

    #to postgres
    print('Rows inserted:')
    print(c)

    fin = time.time()
    print("execution time")
    print(fin - inicio)


    ##Reporte