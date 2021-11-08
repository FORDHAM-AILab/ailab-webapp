import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
import csv
import sys
import logging
import json
import os
import pathlib
from python_aws.config_parser import parse_config
from functools import lru_cache
from python_aws.utils import ProgressPercentage
import pandas as pd
from typing import Optional, NoReturn, List

# initiating logger
logger = logging.getLogger()
logger.setLevel(logging.WARNING)


@lru_cache()
def get_client(**kwargs):
    """
    Return a singleton of S3 client
    """
    if kwargs is not None:
        s3_config = Config(**kwargs)
        return boto3.client('s3', config=s3_config)
    else:
        return boto3.client('s3')


@lru_cache()
def get_resource():
    """
    Return a singleton of S3 resource
    """
    return boto3.resource('s3')


def list_buckets() -> Optional[dict]:
    """
    List all buckets in S3
    """
    s3c = get_client()
    return s3c.list_buckets()


def get_bucket(bucket_name: str):
    """
    Return the handle for an S3 bucket.
    Input:
        bucket_name:    name of the S3 bucket where we want to operate
    Errors will be thrown if
        (1) No credential
        (2) Not found
        (3) Bucket name not unique
    """
    try:
        # Check if you have permissions to access the bucket
        s3 = get_resource()
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except NoCredentialsError as e:
        logging.warning("No Valid Credentials", exc_info=True)
        sys.exit()
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            logger.error("bucket not found: %s" % bucket_name)
        else:
            logger.error("Specify a unique bucket name", exc_info=True)
        sys.exit()
    else:
        return s3.Bucket(bucket_name)


def check_object_exists(bucket_name: str, key: str) -> bool:
    """
    Check whether an object (file) exists under an S3 bucket.
    Input:
        bucket_name:    name of bucket where this function searches
        key:            name of the object (file)
    Return:
        True/False:     whether this Key exists or not
    """
    s3_bucket = get_bucket(bucket_name)
    for item in s3_bucket.objects.all():
        if key == item.key:
            return True
    return False


def check_bucket_exists(bucket_name: str) -> int:
    """
    Check whether a bucket exists in S3
    Input:
        bucket_name:    name of the bucket you want to check
    Output:
        -1:             No permission
        0:              Does not exist
        1:              Exist
    """
    # check_status is -1 if no permissions, 0 if doesn't exist and 1 if it exists
    check_status = -1
    try:
        # Check if you have permissions to access the bucket
        s3 = get_resource()
        s3.meta.client.head_bucket(Bucket=bucket_name)
        check_status = 1
    except NoCredentialsError as e:
        logging.warning("No Valid Credentials", exc_info=True)
        sys.exit()
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            check_status = 0
        else:
            logger.error("Specify a unique bucket name", exc_info=True)
            sys.exit()
    finally:
        return check_status


def create_bucket(bucket_name: str, region: Optional[str] = None, sse: Optional[dict] = None) -> bool:
    """
    Bucket does not exist, so create it.
    Input:
        bucket_name:    name of the bucket to create
        region:         AWS region where this bucket sits.  Default to us-east-1
        sse:            security/authentication setting
    Output:
        True/False:     whether or not this creation succeeds
    Do not specify a LocationConstraint if the region is us-east-1 -
    S3 does not like this!!
    """
    if region is None:
        cfg = parse_config()
        region = cfg["region_config"]
    if sse is None:
        cfg = parse_config()
        sse = cfg["sse_config"]

    if check_bucket_exists(bucket_name) == 0:
        create_bucket_config = {}
        s3 = get_resource()

        if region != "us-east-1":
            create_bucket_config["LocationConstraint"] = region
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=create_bucket_config, ACL='private')
        else:
            s3.create_bucket(Bucket=bucket_name, ACL='private')

        try:
            put_bucket_encryption(bucket_name, sse)
            put_public_access_block(bucket_name)
        except Exception as e:
            logger.error(e)

        logger.info('Created bucket: %s ', bucket_name)

        return True
    else:
        return False


def put_bucket_encryption(bucket_name: str, sse: Optional[dict] = None) -> dict:
    """
    Add encryption to a S3 bucket.
    Input:
        bucket_name:    name of the bucket to be encrypted
        sse:            security settings, need to have keyid for the encryption
    Output:
        response:       response from s3_client (boto3 pkg)
    """
    s3c = get_client()
    if sse is None:
        cfg = parse_config()
        sse = cfg["sse_config"]
    try:
        response = s3c.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                'Rules': [
                    {
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'aws:kms',
                            'KMSMasterKeyID': sse['keyid']
                        }
                    }
                ]
            }
        )
    except Exception as e:
        logger.error(e)
        response = None

    return response


def put_bucket_version(bucket_name: str) -> NoReturn:
    """
    Enable bucket version.  No return.
    """
    s3c = get_client()
    s3c.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={
            'Status': 'Enabled'
        }
    )


def create_bucket_policy(bucket_name: str) -> dict:
    """
    Create a default policy and assign this policy to the bucket
    Input:
        bucket_name:    name of the bucket to be added a policy
    Output:
        None
    """
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AddPerm",
                "Effect": "Allow",
                "Principal": "*",
                "Action": [
                    's3:DeleteObject',
                    's3:GetObject',
                    's3:PutObject'
                ],
                "Resource": 'arn:aws:s3:::' + bucket_name + '/*'
            }
        ]
    }

    policy_string = json.dumps(bucket_policy)
    s3c = get_client()

    return s3c.put_bucket_policy(
        Bucket=bucket_name,
        Policy=policy_string
    )


def get_bucket_policy(bucket_name: str) -> dict:
    """
    Return bucket policy for a given bucket
    """
    s3c = get_client()
    return s3c.get_bucket_policy(Bucket=bucket_name)


def get_bucket_encryption(bucket_name: str) -> dict:
    """
    Return bucket encryption config/setting for a given bucket
    """
    s3c = get_client()
    return s3c.get_bucket_encryption(Bucket=bucket_name)


def version_bucket_files(bucket_name: str) -> NoReturn:
    """
    Enable version for a given bucket. No returns
    """
    s3c = get_client()
    s3c.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={
            'Status': 'Enabled'
        }
    )


def put_public_access_block(bucket_name: str) -> dict:
    """
    Block public access for a S3 bucket by adding a block policy.  No returns
    """
    s3c = get_client()
    response = s3c.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': True,
            'IgnorePublicAcls': True,
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True
        }
    )
    return response


def delete_bucket(bucket_name: str) -> NoReturn:
    """
    Delete a S3 bucket
    """
    try:
        # Check if you have permissions to access the bucket
        bucket = get_bucket(bucket_name)
        bucket.delete()
    except Exception as e:
        logger.error("Couldn't delete this bucket %s", bucket_name)


def get_object(bucket_name: str, key: str) -> dict:
    """
    Return the handle for a key in a S3 bucket
    """
    s3c = get_client()
    try:
        obj = s3c.get_object(bucket_name, key)
    except Exception as e:
        logger.error("bucket/key couldnt be found or accessed")
    else:
        return obj


def put_object(bucket_name: str, file: str, key: str, sse: Optional[dict] = None) -> dict:
    """
    Upload a file into S3 bucket.  Use boto3.s3 function put_object.
    File is uploaded in one piece, capped at 5 Gb, can be tracked.  Check BOTO3 docs for more details
    Input:
        bucket_name:    specify which bucket to upload this file
        file:           path for the local file to be uploaded
        key:            key name in S3 bucket.  It could be different than the local filename
    Output:
        response:       status of the upload
    """
    s3c = get_client()
    if sse is None:
        cfg = parse_config()
        sse = cfg["sse_config"]

    try:
        response = s3c.put_object(bucket_name,
                                  key,
                                  Body=file,
                                  ServerSideEncryption="aws:kms",
                                  SSEKMSKeyID=sse["keyid"]
                                  )
    except Exception as e:
        logger.error("error in writing object")
        response = None
    return response


def delete_object(bucket_name: str, key: str) -> bool:
    """
    Delete an object (key) from a S3 bucket.
    Return the status of this deletion: True(Succeed) /False(Failed)
    """
    s3c = get_client()
    flag = False
    try:
        s3c.delete_object(Bucket=bucket_name, Key=key)
        flag = True
    except Exception as e:
        logger.error(e)
    return flag


def upload_file_to_bucket(bucket_name: str, file_name: str,
                          key_name: Optional[str] = None, sse: Optional[dict] = None) -> bool:
    """
    Upload a file into S3 bucket.  Use boto3.s3 function upload_file.
    File is uploaded in multi-parts, capped at 10 upload threads simultaneously. Unlike put_object(), this upload
    can NOT be tracked.  Check BOTO3 docs for more details
    Input:
        bucket_name:    specify which bucket to upload this file
        file:           path for the local file to be uploaded
        key_name:       key name in S3 bucket.  It could be different than the local filename
        sse:            security config/setting
    Output:
        return True if the upload job goes successfully
    """
    config = TransferConfig(multipart_threshold=1024 * 25,
                            max_concurrency=10,
                            multipart_chunksize=1024 * 25,
                            use_threads=True
                            )
    file_path_obj = pathlib.Path(__file__).parent.joinpath(file_name)
    file_path = str(file_path_obj.resolve())
    key_name = key_name if key_name is not None else file_name
    if sse is None:
        cfg = parse_config()
        sse = cfg["sse_config"]
    s3 = get_resource()
    try:
        s3.meta.client.upload_file(file_path,
                                   bucket_name,
                                   key_name,
                                   ExtraArgs={
                                       'ACL': 'private',
                                       'ContentType': 'text/csv',
                                       'ServerSideEncryption': 'aws:kms',
                                       'SSEKMSKeyId': sse["keyid"]
                                   },
                                   Config=config,
                                   Callback=ProgressPercentage(file_path)
                                   )
    except ClientError as e:
        logger.error(e)
    else:
        return True


def download_file_from_bucket(bucket_name: str, key: str, file_name: Optional[str] = None) -> bool:
    """
    Download file(key) from bucket.
    Input:
        bucket_name:    name of the bucket where the key sits
        key:            key name
        file_name:      name of the download file
    Output:
        return True if the file is downloaded without any exceptions
    """
    bucket = get_bucket(bucket_name)
    if file_name is None:
        file_name = key

    try:
        bucket.download_file(key, file_name)
    except Exception as e:
        logger.error(e)
    else:
        return True


def list_objects_in_bucket(bucket_name: str) -> List[str]:
    """
    Return a list of all objects in a S3 bucket
    # this assumes data is csv files with headers
    # Get summary information for all objects in input.py bucket
    # Iterate over the list of object summaries
    """
    result = []
    bucket = get_bucket(bucket_name)
    for object_summary in bucket.objects.all():
        # Get the object key from each object summary
        key = object_summary.key
        result.append(key)
    return result


def read_input_bucket_key(bucket_name: str, key = str, headings: bool = True,
                          remove_tmp_file: bool = True) -> csv.DictReader:
    """
    Download an object from bucket, load it into memory, and return a handle of csv.DictReader() type
    Input:
        bucket_name:    name of the bucket where this object sits
        key:            object name in the S3 bucket
        headings:       True/False - whether to use the first line of the object file as the heading (column name)
        remove_tmp_file:True/False - whether to remove the temporacy download
    Output:
        return a handle of the object in csv.DictReader format
    """
    temp_file_name = "tmp-" + str(hash(bucket_name + key))
    download_file_from_bucket(bucket_name, key, temp_file_name)
    # open the file
    file = open(temp_file_name, 'r')
    # Get the headings
    if headings:
        heading = file.readlines(1)[0].split(',')
        reader = csv.DictReader(file, fieldnames=heading)
    else:
        reader = csv.DictReader(file)

    fname = file.name
    file.close()
    if remove_tmp_file is True:
        os.remove(fname)
    return reader


def read_file_pandas(bucket_name: str, key: str) -> pd.core.frame.DataFrame:
    """
    Read an object into pandas data format, and print it out.
    """
    try:
        obj = get_object(bucket_name, key)
    except ClientError as e:
        logger.error(e)
    obj_data = pd.read_csv(obj['Body'])
    print(obj_data)


def generate_presigned_url(bucket: str, key: str, expiration: int, method: str = 'get_object', **kwargs):
    """
    Generate pre-signed URL which any users can use to download/upload/modify S3 objects
    @param bucket: S3 bucket name
    @type bucket: string
    @param key: object key in S3 bucket
    @type key: basestring
    @param expiration: How long this temporary authentication
    @type expiration: int
    @param method: Method name, download, upload, modify, or something else?
    @type method: string
    @param kwargs: Any additional arguments that go into boto3 function generate_preassign_url
    @type kwargs:
    @return:
    @rtype:
    """
    # Asof July 2021, S3 required AWS Signature Version 4 for presigned URL
    s3_client = get_client(signature_version='s3v4')
    url_params = {'Bucket': bucket, 'Key': key}
    url_params.update(kwargs)
    url = s3_client.generate_presigned_url(Params=url_params, ClientMethod=method, ExpiresIn=expiration)
    return url
