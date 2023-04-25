from upload_aws import get_authenticated_service, theme_block, stop_notification, create_notfication


def lambda_handler(event, context):
    # TODO implement
    drive = get_authenticated_service()
    objects = theme_block()
    keys_dic = list(objects[0].keys())
    count = 0
    for key in keys_dic:
        ids_list = objects[0][key]
        url = objects[1][key]
        for id_ in ids_list:
            count += 1
            create_notfication(id_, drive, url, key)
            # stop_notification(id_,drive,url,key)

    print(count)

    return {
        'statusCode': 200
    }