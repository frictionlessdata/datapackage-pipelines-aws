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

    is_truncated = True
    contents = []
    marker = ''
    # list_objects returns max 1000 keys (even if MaxKeys is >1000)
    while is_truncated:
        try:
            objs = s3.list_objects(Bucket=bucket, Prefix=key, Marker=marker)
            is_truncated = objs.get('IsTruncated')
            contents += objs.get('Contents', [])
            if len(contents):
                marker = contents[-1]['Key']
        except Exception:
            is_truncated = False
    keys = [content['Key'] for content in contents]
    for obj in keys:
        s3.put_object_acl(Bucket=bucket, Key=obj, ACL=acl)


change_acl()

spew(datapackage, resource_iterator)
