import boto3, os
from datapackage_pipelines.lib.dump.dumper_base import FileDumper

from datapackage_pipelines_aws.helpers import generate_path


class S3Dumper(FileDumper):

    def initialize(self, params):
        super(S3Dumper, self).initialize(params)
        self.bucket = params['bucket']
        self.acl = params.get('acl', 'public-read')
        self.client = boto3.client('s3', endpoint_url=os.environ.get("S3_ENDPOINT_URL"))
        self.base_path = params.get('path', '')
        self.content_type = params.get('content_type', 'text/plain')

    def prepare_datapackage(self, datapackage, params):
        super(S3Dumper, self).prepare_datapackage(datapackage, params)
        self.datapackage = datapackage
        return datapackage

    def write_file_to_output(self, filename, path, allow_create_bucket=True):
        key = generate_path(path, self.base_path, self.datapackage)
        try:
            self.client.put_object(
                ACL=self.acl,
                Body=open(filename, 'rb'),
                Bucket=self.bucket,
                ContentType=self.content_type,
                Key=key)
        except self.client.exceptions.NoSuchBucket:
            if os.environ.get("S3_ENDPOINT_URL") and allow_create_bucket:
                # if you provided a custom endpoint url, we assume you are using a s3 compatible server
                # in this case, creating a bucket should be cheap and easy, so we can do it here
                self.client.create_bucket(Bucket=self.bucket)
                self.write_file_to_output(filename, path, allow_create_bucket=False)
            else:
                raise
