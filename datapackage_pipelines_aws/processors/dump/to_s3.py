import boto3

from datapackage_pipelines.lib.dump.dumper_base import CSVDumper
from datapackage_pipelines_aws import helpers


class S3Dumper(CSVDumper):

    def initialize(self, params):
        super(S3Dumper, self).initialize(params)
        self.bucket = params['bucket']
        self.acl = params.get('acl', 'public-read')
        self.client = boto3.client('s3')
        self.base_path = params.get('path', '')

    def prepare_datapackage(self, datapackage, _):
        self.datapackage = datapackage
        return datapackage

    def write_file_to_output(self, filename, path):
        key = helpers.generate_path(path, self.base_path, self.datapackage)
        self.client.put_object(
            ACL=self.acl,
            Body=open(filename, 'rb'),
            Bucket=self.bucket,
            Key=key)


S3Dumper()()
