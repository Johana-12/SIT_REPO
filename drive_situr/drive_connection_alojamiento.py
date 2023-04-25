import json
from io import BytesIO
import pandas as pd
import boto3
from upload_aws import upload_file, save_database, get_authenticated_service
import time


def attachment_drive(drive, id_, name_file):
    '''Gets the Google Drive file, corresponding to the id that reported it as being modified
    Params:
        input: googleapiclient, file id ,
        output: Store attachment in s3 bucket, in json format, and in RDS database_otm1'''

    files = drive.files().get_media(fileId=id_).execute()
    df = pd.read_excel(BytesIO(files))
    df.dropna(axis=0, how='all', inplace=True)
    id_column = df.columns[0]
    df = df.tail()

    table_name = 'TBL_' + name_file[:-5]
    # print(table_name)
    list_record = df.to_dict('records')
    for record in list_record:
        save_database(table_name, record)
    return


def lambda_handler(event, context):
    drive = get_authenticated_service()
    file_updated = drive.files().list(orderBy='modifiedTime desc').execute()
    # time.sleep(5)

    id_file = file_updated['files'][0]['id']
    name_file = file_updated['files'][0]['name']
    print(id_file, name_file)
    list_id = ['1699bHG_ss5TlOIqW9BJFlO9_5BHFmCzh',  # ocupacionhotelera
               '1XPN6ZnpnX9OeD0UpZU23D8UQJVlbzvHg',  # ocupacionhotelerazonas
               '17ZIgFro1Jj_ifZdwiZNN4od69tSMMGhC',  # tarifapromedio
               '15HKIIjA6qYscUCZPvfKg9RfCOHJn2sqw'  # tarifapromediozona
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
#     time.sleep(40)
#     id_file = file_updated['files'][0]['id']
#     name_file = file_updated['files'][0]['name']
#     list_id = ['1699bHG_ss5TlOIqW9BJFlO9_5BHFmCzh',#ocupacionhotelera
#                                 '1XPN6ZnpnX9OeD0UpZU23D8UQJVlbzvHg',#ocupacionhotelerazonas
#                                 '17ZIgFro1Jj_ifZdwiZNN4od69tSMMGhC',#tarifapromedio
#                                 '15HKIIjA6qYscUCZPvfKg9RfCOHJn2sqw'#tarifapromediozona
#                                     ]
#     list_name = ['OCUPACIONHOTELERA.xlsx','OCUPACIONHOTELERAZONAS.xlsx','TARIFAPROMEDIO.xlsx','TARIFAPROMEDIOZONA.xlsx']
#     for id_file,name_file in zip(list_id,list_name):
#         attachment_drive(drive,id_file,name_file)
#         print(f'se modificó el archivo {name_file}: {id_file}')

#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')

#     }