import json
from io import BytesIO
import pandas as pd
import boto3
from upload_aws import upload_file, save_database, get_authenticated_service
import time


def attachment_drive(drive, id_, name_file):
    '''Gets the Drive file, corresponding to the id that reported it as being modified
    Params:
        input: googleapiclient, file id ,
        output: Store attachment in s3 bucket, in json format, and in RDS database_otm1'''

    files = drive.files().get_media(fileId=id_).execute()
    df = pd.read_excel(BytesIO(files))
    df.dropna(axis=0, how='all', inplace=True)
    table_name = 'TBL_' + name_file[:-5]
    list_record = df.to_dict('records')
    for recor in list_record[-15:]:
        save_database(table_name, recor)

    return


def lambda_handler(event, context):
    drive = get_authenticated_service()
    file_updated = drive.files().list(orderBy='modifiedTime desc').execute()
    # time.sleep(20)
    id_file = file_updated['files'][0]['id']
    name_file = file_updated['files'][0]['name']

    list_id = ['1ysPnorBJJPuv9huhyYFgKRVakRemBHL_',  # MUSEOSINGRESO
               '174sy8fVCcdPks563-Q8s7hPVyDknQUlh',  # pitsingreso
               '1WWXDn5Xi1K2X45PTsfv-3_p2vUOcvDUI'  # sitiosturisticosingresomensual
               ]

    if id_file in list_id:
        attachment_drive(drive, id_file, name_file)
        print(f'se modificó el archivo {name_file}: {id_file}')

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

# def lambda_handler(event, context):
#     drive = get_authenticated_service()
#     file_updated = drive.files().list(orderBy='modifiedTime desc').execute()
#     #time.sleep(40)
#     id_file = file_updated['files'][0]['id']
#     name_file = file_updated['files'][0]['name']
# list_id = [
#     '1ysPnorBJJPuv9huhyYFgKRVakRemBHL_',#MUSEOSINGRESO
#             '174sy8fVCcdPks563-Q8s7hPVyDknQUlh',#pitsingreso
#             '1WWXDn5Xi1K2X45PTsfv-3_p2vUOcvDUI'#sitiosturisticosingresomensual
#             ]
#     list_name = [
#         'MUSEOSINGRESO.xlsx',
#         'PITSINGRESOS.xlsx',
#         'SITIOSTURISTICOSINGRESOMENSUAL.xlsx'
#         ]
#     for id_file,name_file in zip(list_id,list_name):
#         attachment_drive(drive,id_file,name_file)
#         print(f'se modificó el archivo {name_file}: {id_file}')


#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }