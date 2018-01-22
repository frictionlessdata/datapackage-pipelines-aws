import os
import boto3
from botocore import errorfactory  # noqa

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, resource_iterator = ingest()


def change_acl():
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    s3 = boto3.client('s3', endpoint_url=endpoint_url)
    bucket = parameters['bucket']
    key = parameters.get('path', '')
    acl = parameters['acl']
    try:
        objs = s3.list_objects(Bucket=bucket, Prefix=key)
    except Exception:
        objs = {}
    contents = objs.get('Contents', [])
    keys = [content['Key'] for content in contents]
    for obj in keys:
        s3.put_object_acl(Bucket=bucket, Key=obj, ACL=acl)


change_acl()

spew(datapackage, resource_iterator)
