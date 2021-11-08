import datetime
import logging

from botocore.exceptions import ClientError
import re
from python_aws import s3
from python_aws.utils import save_upload_file
from fastapi import FastAPI, File, UploadFile
import uvicorn
import traceback
import CONSTS
import boto3

from python_aws.generic import ResultResponse

app = FastAPI()

@app.post("/s3/get_file_structure/bucket/key", tags=['s3'])
async def get_file_structure(bucket_name: str, key: str):
    try:
        result = []
        s3_obj = boto3.resource('s3')
        my_bucket = s3_obj.Bucket(bucket_name)
        for object_summary in my_bucket.objects.filter(Prefix=key):
            file_dict = {}
            if object_summary.key != key and object_summary.key != key + '/':
                file_dict['name'] = [i for i in object_summary.key.split('/') if i != ''][-1]
                file_dict['path'] = object_summary.key
                if object_summary.key[-1] == '/':
                    file_dict['IsDirectory'] = True
                    if len(list(my_bucket.objects.filter(Prefix=object_summary.key))) >1:
                        file_dict['HasChild'] = True

                    else:
                        file_dict['HasChild'] = False
                else:
                    file_dict['IsDirectory'] = False
                    file_dict['HasChild'] = False
                #     file_dict['HasChild'] = False
                # location = boto3.client('s3').get_bucket_location(Bucket=bucket_name)['LocationConstraint']
                # url = "https://s3-%s.amazonaws.com/%s/%s" % (location, bucket_name, object_summary.key)
                url = s3.generate_presigned_url(bucket = bucket_name, key = object_summary.key, expiration=CONSTS.PRESIGNED_URL_LIFE)
                file_dict['FileUrl'] = url
            result.append(file_dict)

    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"get file structure {bucket_name}/{key}", result = result,
                              date_done=str(datetime.datetime.utcnow().isoformat()))


@app.post("/s3/upload_s3_object/bucket/object", tags=['s3'])
async def upload_s3_object(bucket: str, key: str, file: UploadFile = File(...)
                           # ,current_user: User = Depends(get_current_user)
                           ):
    file_name = save_upload_file(file, CONSTS.TEMP_PATH.joinpath(file.filename))
    try:
        s3.upload_file_to_bucket(bucket, file_name, key)
    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Upload file to {bucket}/{key}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))

async def delete(bucket_name: str,  isDirectory: bool, key: str):
    try:

        s3_object = boto3.resource('s3')
        objects_to_delete = s3_object.meta.client.list_objects(Bucket=bucket_name, Prefix=key)

        delete_keys = {'Objects': []}
        delete_keys['Objects'] = [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]


        s3_object.meta.client.delete_objects(Bucket=bucket_name, Delete=delete_keys)

    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Delete {'directory' if isDirectory else 'file'} {bucket_name}/{key}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))

@app.post("/s3/delete_file/bucket/key", tags=['s3'])
async def delete_file(bucket_name: str,  isDirectory: bool, key: str):
    return await delete(bucket_name,isDirectory,key)


@app.post("/s3/move_file/bucket/key/", tags=['s3'])
async def move_file(bucket_name: str, key: str, isDirectory: bool, path: str):
    try:
        s3_object = boto3.resource('s3')

        if isDirectory:
            key = key + '/' if key[-1] != '/' else key
            my_bucket = s3_object.Bucket(bucket_name)
            for object_summary in my_bucket.objects.filter(Prefix=key):

                new_key = object_summary.key.replace(key,path)[:-1] if object_summary.key.replace(key,path)[-2:] == '//' else object_summary.key.replace(key,path)

                s3_object.Object(bucket_name, new_key).copy_from(CopySource=f'{bucket_name}/{object_summary.key}')
            await delete(bucket_name, True, key)
        ##
        else:
            s3_object.Object(bucket_name, path).copy_from(CopySource=f'{bucket_name}/{key}')
            await delete(bucket_name, isDirectory, key)

    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Moved file {bucket_name}/{key} --> to --> {bucket_name}/{path}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))

@app.post("/s3/rename_file/bucket/key/newname", tags=['s3'])
async def rename_file(bucket_name: str, key: str, isDirectory: bool,  newname:str, path: str):
    try:
        s3_object = boto3.resource('s3')

        if isDirectory:
            key = key + '/' if key[-1] != '/' else key
            file_name_index = len([i for i in re.split('(/)', key) if i != '']) - 2
            my_bucket = s3_object.Bucket(bucket_name)
            for object_summary in my_bucket.objects.filter(Prefix=key):
                new_key_splitted = [i for i in re.split('(/)', object_summary.key) if i != '']
                new_key_splitted[file_name_index] = newname
                new_key = ''.join(new_key_splitted)
                s3_object.Object(bucket_name, new_key).copy_from(CopySource=f'{bucket_name}/{object_summary.key}')
                s3_object.Object(bucket_name, object_summary.key).delete()

        else:
            s3_object.Object(bucket_name, path).copy_from(CopySource=f'{bucket_name}/{key}')
            s3_object.Object(bucket_name,key).delete()

    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Rename {'directory' if isDirectory else 'file'} {bucket_name}/{key} --> to --> {bucket_name}/{path} ",
                              date_done=str(datetime.datetime.utcnow().isoformat()))


@app.post("/s3/create_s3_bucket/bucket/key", tags=['s3'])
async def create_s3_bucket(bucket_name: str):
    try:

        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket_name)

    except ClientError as e:
        logging.error(e)
        return False
    return True

@app.post("/s3/create_s3_dir/bucket/key", tags=['s3'])
async def create_s3_dir(bucket_name: str, key: str):
    try:
        s3 = boto3.client('s3')
        key = key + '/' if key[-1] != '/' else key
        s3.put_object(Bucket=bucket_name, Body='', Key = key)
    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Create directory to {bucket_name}/{key}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))

@app.post("/s3/create_s3_file/bucket/key", tags=['s3'])
async def create_s3_file(bucket_name: str, key: str):
    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Body='', Key = key)
    except Exception as e:
        return ResultResponse(status='failed', message=f"{str(e)}:\n{traceback.format_exc()}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))
    else:
        return ResultResponse(status='succeed', message=f"Create file to {bucket_name}/{key}",
                              date_done=str(datetime.datetime.utcnow().isoformat()))

if __name__ == '__main__':
    uvicorn.run('app:app', port=8000, host='127.0.0.1', log_level="info", reload=True)