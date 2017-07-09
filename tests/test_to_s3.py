import json
import os
import unittest
import datetime

from moto import mock_s3
import boto3

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_aws.processors

import logging
log = logging.getLogger(__name__)


class TestToS3Proccessor(unittest.TestCase):
    def setUp(self):
        self.bucket = 'my.test.bucket'
        self.resources = [{
            'name': 'resource',
            "format": "csv",
            "path": "data/test.csv",
            "schema": {
                "fields": [
                    {
                        "name": "Date",
                        "type": "date",
                    },
                    {
                        "name": "Name",
                        "type": "string",
                    }
                ]
            }
         }]
        self.datapackage = {
            'owner': 'me',
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': self.resources
        }
        self.params = {
            'bucket': self.bucket,
            'path': 'my/test/path/{owner}/{name}/{version}'
        }
        # Path to the processor we want to test
        self.processor_dir = \
            os.path.dirname(datapackage_pipelines_aws.processors.__file__)
        self.processor_path = os.path.join(self.processor_dir, 'to_s3.py')

    @mock_s3
    def test_puts_datapackage_on_s3(self):
        # Should be in setup but requires mock
        s3 = boto3.resource('s3')
        s3.create_bucket(Bucket=self.bucket)
        bucket = s3.Bucket(self.bucket)

        class TempList(list):
            pass

        res =  TempList([{'Date': datetime.datetime(1, 1, 1), 'Name': 'Name'}])
        res.spec = self.resources[0]
        res_iter = [res]

        spew_args, _ = mock_processor_test(self.processor_path,
                            (self.params,
                            self.datapackage,
                            res_iter))

        spew_res_iter = spew_args[1]
        # We need to actually read the rows to ecexute the iterator(s)
        rows = [list(res) for res in spew_res_iter]

        keys = [key.key for key in bucket.objects.all()]

        dp_path = 'my/test/path/me/my-datapackage/latest/datapackage.json'
        csv_path = 'my/test/path/me/my-datapackage/latest/data/test.csv'
        assert dp_path in keys
        assert csv_path in keys

        # Check datapackage.json content
        content = s3.Object(self.bucket, dp_path).get()['Body']\
            .read().decode("utf-8")
        self.assertEquals(json.loads(content)['owner'], 'me')
        self.assertEquals(json.loads(content)['name'], 'my-datapackage')

        # Check csv content
        content = s3.Object(self.bucket, csv_path).get()['Body']\
            .read().decode("utf-8")
        expected_csv = 'Date,Name\r\n0001-01-01 00:00:00,Name\r\n'
        self.assertEquals(content, expected_csv)
