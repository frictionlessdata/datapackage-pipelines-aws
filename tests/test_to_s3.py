import json
import os
import unittest

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
        s3 = boto3.resource('s3')
        s3.create_bucket(Bucket=self.bucket)

        mock_processor_test(self.processor_path,
                            (self.params,
                            self.datapackage,
                            iter([])))

        keys = []
        for bucket in s3.buckets.all():
            for key in bucket.objects.all():
                keys.append(key.key)

        self.assertEquals(len(keys), 1)
        res_path = 'my/test/path/me/my-datapackage/latest/datapackage.json'
        self.assertEqual(res_path,keys[0])

        content = s3.Object(self.bucket, res_path).get()['Body']\
            .read().decode("utf-8")
        self.assertDictEqual(json.loads(content), self.datapackage)
