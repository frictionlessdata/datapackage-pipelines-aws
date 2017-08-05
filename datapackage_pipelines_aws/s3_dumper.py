import boto3
from datapackage_pipelines.lib.dump.dumper_base import FileDumper

from datapackage_pipelines_aws.helpers import generate_path


class S3Dumper(FileDumper):

    def initialize(self, params):
        super(S3Dumper, self).initialize(params)
        self.bucket = params['bucket']
        self.acl = params.get('acl', 'public-read')
        self.client = boto3.client('s3')
        self.base_path = params.get('path', '')
        self.content_type = params.get('content_type', 'text/plain')

    def prepare_datapackage(self, datapackage, params):
        super(S3Dumper, self).prepare_datapackage(datapackage, params)
        self.datapackage = datapackage
        return datapackage

    def write_file_to_output(self, filename, path):
        key = generate_path(path, self.base_path, self.datapackage)
        self.client.put_object(
            ACL=self.acl,
            Body=open(filename, 'rb'),
            Bucket=self.bucket,
            ContentType=self.content_type,
            Key=key)
