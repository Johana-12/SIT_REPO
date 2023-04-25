import boto3
from botocore.exceptions import ClientError
import logging
import json
import pickle
from googleapiclient.discovery import build
import re
from datetime import datetime, timedelta, date


def upload_file(file_name, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # Upload the file
    client = boto3.client('s3')
    try:
        response = client.put_object(Bucket='datos-situr-otm',
                                     Key="{}".format(file_name),
                                     Body=json.dumps(object_name)
                                     )
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_authenticated_service():
    '''Access token stored in s3 bucket archivos-apis-google
    output: Connection to googleapiclient service'''
    client = boto3.client('s3')
    response = client.get_object(Bucket='archivos-apis-google', Key='tokendrivetecnico.pickle')
    body = response['Body'].read()
    creds = pickle.loads(body)
    return build('drive', 'v3', credentials=creds)


def period_date(period):
    """Divide period data into month, year an date
    INPUT:
        period: period data
    OUTPUT:
        month: month contained in period data
        year: year contained in period data
        date: date contained in period data"""
    try:
        date = datetime.strptime(period, '%Y%m')
    except:
        date = datetime.strptime(period, '%Y')

    day = date.day
    month = date.month
    year = date.year
    date_ = f'{day}/{month}/{year}'
    return month, year, date_


def save_database(table, record):
    """save data into database Aurora postgresql serverless
    INPUT:
        table: nombre tabla
        record: diccionario

    OUTPUT:
        response """
    database = 'database_otm1'
    secret_arn = 'arn:aws:secretsmanager:us-east-2:548835178718:secret:rds-db-credentials/cluster-I7PZS6AIMFRK2XUM5JIRT7WH5A/postgres-O9xGxX'
    cluster_arn = 'arn:aws:rds:us-east-2:548835178718:cluster:database-otm'
    secret_arn = 'arn:aws:secretsmanager:us-east-2:548835178718:secret:rds-db-credentials/cluster-ZZLHI2ESFHPJUQD2O2HYC5L7IU/postgres-6x8FFb'
    rdsData = boto3.client('rds-data',
                           region_name='us-east-2')

    values = tuple(map(lambda x: re.sub('\.0+', '', str(x)), list(record.values())))
    keys = list(record.keys())
    try:
        response1 = rdsData.execute_statement(
            resourceArn=cluster_arn,
            secretArn=secret_arn,
            database=database,
            sql=f"INSERT INTO {table} VALUES {values};")

    except:
        text = ''
        for key, value in zip(keys, values):
            text += f"{key} = '{value}',"
        data = re.sub(',$', '', text)
        # print(data)
        # print(keys[0],values[0])
        response1 = rdsData.execute_statement(
            resourceArn=cluster_arn,
            secretArn=secret_arn,
            database=database,
            sql=f"UPDATE {table} SET {data} WHERE {keys[0]}={values[0]};")

    for key in keys:
        if 'PERIODO' in key:
            tab = key[:4]
            period = int(float(record[key]))
            record[key] = period
            periodate = period_date(str(period))
            month = periodate[0]
            year = periodate[1]
            date_ = periodate[2]
            n_month = tab + 'MES'
            n_year = tab + 'ANIO'
            n_date = tab + 'FECHA'
            record[n_month] = month
            record[n_year] = year
            record[n_date] = date_
        elif '_ID' in key:
            record[key] = int(float(record[key]))

        elif 'NACIONALIDAD' in key:

            nation = str(record[key])
            if 'EXTRAN' in nation:
                record[key] = 'Extranjero'
            else:
                record[key] = 'Colombiano'
        elif 'TOTALVISITAS' in key:
            record[key] = int(record[key])

    file_name = 'atracciones/' + table + '/' + keys[0] + values[0] + '.json'
    upload_file(file_name, record)

    return

import boto3
from botocore.exceptions import ClientError
import logging
import json
import pickle
from googleapiclient.discovery import build
import re


def upload_file(file_name, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # Upload the file
    client = boto3.client('s3')
    try:
        response = client.put_object(Bucket='archivos-apis-google',
                                     Key="{}".format(file_name),
                                     Body=json.dumps(object_name)
                                     )
    except ClientError as e:
        logging.error(e)
        return False
    return True


def get_authenticated_service():
    '''Acceder al token de acceso almacenado en bucket de s3 archivos-apis-google
    output: Conexión al servicio googleapiclient'''
    client = boto3.client('s3')
    response = client.get_object(Bucket='archivos-apis-google', Key='tokendrivetecnico.pickle')
    body = response['Body'].read()
    creds = pickle.loads(body)
    return build('drive', 'v3', credentials=creds)


def theme_block():
    '''Obtiene diccionario con los id de cada archivo de SITUR en el Google Drive de tecnico@turismomde.gov.com
    output: Tupla con json de id de archivos y json de endpoints para notificación de cambios en los archivos
    '''
    ids_files = {'alojamiento': ['1699bHG_ss5TlOIqW9BJFlO9_5BHFmCzh',  # ocupacionhotelera
                                 '1XPN6ZnpnX9OeD0UpZU23D8UQJVlbzvHg',  # ocupacionhotelerazonas
                                 '17ZIgFro1Jj_ifZdwiZNN4od69tSMMGhC',  # tarifapromedio
                                 '15HKIIjA6qYscUCZPvfKg9RfCOHJn2sqw'  # tarifapromediozona
                                 ],
                 'atracciones': ['1ysPnorBJJPuv9huhyYFgKRVakRemBHL_',  # MUSEOSINGRESO
                                 '174sy8fVCcdPks563-Q8s7hPVyDknQUlh',  # PITSINGRESOS
                                 '1WWXDn5Xi1K2X45PTsfv-3_p2vUOcvDUI'  # SITIOSTURISTICOSINGRESOMENSUAL
                                 ],
                 'conectividad': ['15oHx9sGoaNJO1JPw8pvbPy05xtlrO0Wn',  # INGRESOVEHICULOS
                                  '1d8fu8aj-KBqczxs2nkkDMJhhJKTA9WOu',  # INGRESOEXTRANJEROSYCOLOMBIANOSAEROPUERTOJMC
                                  '1xSaiAzlGgTqVHd9Jx2omgq3BZGsiF2Q0',
                                  # INGRESOMENSUALEXTRANJEROSPUESTOCONTROLMIGRATORIO
                                  '1Qa_2eGGA2JajKX6CtYjUk5zzeRfaF2B1',  # INGRESOPASAJEROSPORCATEGORIA
                                  '1NMyBExxmAerMWYsmFoY2LNvmHMgTbuRn',
                                  # LLEGADAMENSUALPASAJEROSPORPAISRESIDENCIA----------------
                                  '1MgV7JqpKzWu_6WC5dGrwYCzqMq42UkHs',  # LLEGADAPASAJEROSAEROPUERTOSORIGENNACIONAL
                                  '1eitIJRe0V3N9MxudtbqrZJBRAJT4mj_r',  # LLEGADAPASAJEROSAEROPUERTOSDEORIGENNACIONAL
                                  '1ieV5Fs05mlxrdPtsRXdkOlm4N5MLr2D9',  # PASAJEROSMOVILIZADOSREDMETRO
                                  '1HrvMeOg4aWaHAT27Jv3Kp_NYZQPTTsz7',
                                  # SALIDAMENSUALPASAJEROSAEROPUERTODESTINONACIONAL
                                  '1qUWtKFODrSwCwL-YTkWeERAshks219zn'  # SALIDAPASAJEROSDESTINOINTERNACIONAL
                                  ],

                 'eventos': ['11rBZ7Ckh9qN2tBaUHH1gIDQND2Qy3TTd',  # OCUPACIONHOTELERACOLOMBIAMODA
                             '1Gf1Q54q0SwhxVsQljvwobjaJYAQhHw7n',  # OCUPACIONHOTELERACOLOMBIATEX
                             '1KIW_hHrZj2g55GIHFDyqVrWvTJ0dgAZK',  # OCUPACIONHOTELERAFERIADEFLORES
                             '1g4dcVsvmPqKXiRbMS0NFARfKFCuLtL6X',  # OCUPACIONHOTELERARECESOESCOLAR
                             '1S5hn-7fBloIwzvXldhQVyCg2BBi8tw9K',  # OCUPACIONHOTELERASEMANASANTA
                             '1v9e9-v-IBeuGnjhRt6I-jHXnTIHg5T3A',  # OCUPACIONHOTELERAVACACIONESMITADA
                             ]
                 }

    urls = {'alojamiento': 'https://0b0tami5ih.execute-api.us-east-2.amazonaws.com/alojamiento',
            'atracciones': 'https://3kou9iro4h.execute-api.us-east-2.amazonaws.com/atracciones',
            'conectividad': 'https://4ylnj30uwh.execute-api.us-east-2.amazonaws.com/conectividad',
            'eventos': 'https://19jq8b6u68.execute-api.us-east-2.amazonaws.com/eventos'

            }
    return ids_files, urls


def create_notfication(id_, drive, url, folder):
    '''Crea la noificación cambio de un archivo en Google Drive
        input: id_: identificación del archivo
            drive: Conexión a Google Drive
            url: endpoint que recibe el evento de notificación y ejectuta la función lambda respectiva
            folder: carpeta de bloque temático
        output: None'''
    name_arch = drive.files().get(fileId=id_).execute()['name'][:-5]
    data = {"kind": "api#channel",
            "id": id_,
            "type": "web_hook",
            "token": id_,
            "address": url,
            "params": {
                'ttl': "604800"
            }

            }
    file_name = folder + '/' + name_arch + '.json'
    print(file_name)

    # CREAR NOTIFICACIONES
    # watch = drive.files().watch(fileId=id_,body=data).execute() solo un dia
    # watch = drive.changes().watch(pageToken='1456',fields='*',body=data).execute()# una semana max
    watch = drive.changes().watch(pageToken='1472', fields='*', body=data).execute()
    upload_file(file_name, watch)
    return


def stop_notification(id_, drive, url, folder):
    '''Detiene las noificaciones de cambio de un archivo respec en Google Drive
        input: id_: identificación del archivo
            drive: Conexión a Google Drive
            url: endpoint que recibe el evento de notificación y ejectuta la función lambda respectiva
            folder: carpeta de bloque temático
        output: None'''
    name_arch = drive.files().get(fileId=id_).execute()['name'][:-5]
    file_name = folder + '/' + name_arch + '.json'
    print(file_name)
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket='archivos-apis-google', Key=file_name)
    file_content = response['Body'].read().decode('utf-8')
    dic = json.loads(file_content)
    print(dic)
    # DETENER NOTIFICACIONES
    drive.channels().stop(body=dic).execute()
    return