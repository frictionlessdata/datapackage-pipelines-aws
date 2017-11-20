import json
import os
import unittest
import datetime
import requests

from moto import mock_s3
import boto3

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_aws.processors

import logging
log = logging.getLogger(__name__)

os.environ['AWS_ACCESS_KEY_ID']="HJW6280KHBS2Y105STGG"
os.environ['AWS_SECRET_ACCESS_KEY']="rsAuRptRwTTuSocsdBCRHIToldPkPefpb2Vl/ybG"
os.environ['S3_ENDPOINT_URL']="http://localhost:5000"


class TestToS3Proccessor(unittest.TestCase):
    def setUp(self):
        self.bucket = 'my.test.bucket'
        self.resources = [{
            'name': 'resource',
            "format": "csv",
            "path": "data/test.csv",
            "dpp:streaming": True,
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
            'path': 'my/test/path/{owner}/{name}/{version}',
            'content_type': 'application/my-made-up-media-type'
        }
        # Path to the processor we want to test
        self.processor_dir = \
            os.path.dirname(datapackage_pipelines_aws.processors.__file__)
        self.processor_path = os.path.join(self.processor_dir, 'dump', 'to_s3.py')

    def test_changes_acl(self):
        # Should be in setup but requires mock
        s3 = boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT_URL'])
        bucket = 'my.private.bucket'
        try:
            s3.create_bucket(ACL='public-read', Bucket=bucket)
        except:
            pass

        readme =  'my/private/datasets/README.md'
        dp = 'my/private/datasets/datapackage.json'
        data = 'my/private/datasets/data/mydata.csv'
        other = 'my/non-private/datasets/data/mydata.csv'
        s3.put_object(Body="README", Bucket=bucket, Key=readme, ACL='public-read')
        s3.put_object(Body='{"name": "testing"}', Bucket=bucket, Key=dp, ACL='public-read')
        s3.put_object(Body="col1,col2\n1,2", Bucket=bucket, Key=data, ACL='public-read')
        s3.put_object(Body="col1,col2\n1,2", Bucket=bucket, Key=other, ACL='public-read')

        params = {
            'bucket': bucket,
            'path': 'my/private/datasets',
            'acl': 'private'
        }

        # Make sure they are public
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, readme)
        self.assertEqual(requests.get(url).status_code, 200)
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, dp)
        self.assertEqual(requests.get(url).status_code, 200)
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, data)
        self.assertEqual(requests.get(url).status_code, 200)
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, other)
        self.assertEqual(requests.get(url).status_code, 200)

        processor_dir = os.path.dirname(datapackage_pipelines_aws.processors.__file__)
        processor_path = os.path.join(processor_dir, 'change_acl.py')
        spew_args, _ = mock_processor_test(processor_path,
                            (params,
                             {'name': 'test', 'resources': []},
                             [[]]))

        # Now they should be forbidden
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, readme)
        self.assertEqual(requests.get(url).status_code, 403)
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, dp)
        self.assertEqual(requests.get(url).status_code, 403)
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, data)
        self.assertEqual(requests.get(url).status_code, 403)
        # This one should remain publick
        url = '{}/{}/{}'.format(os.environ['S3_ENDPOINT_URL'], bucket, other)
        self.assertEqual(requests.get(url).status_code, 200)

    def test_puts_datapackage_on_s3(self):
        # Should be in setup but requires mock
        s3 = boto3.resource('s3', endpoint_url=os.environ['S3_ENDPOINT_URL'])
        bucket = s3.Bucket(self.bucket)

        class TempList(list):
            pass

        res =  TempList([{'Date': datetime.datetime(2001, 2, 3), 'Name': 'Name'}])
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
        dpjson = s3.Object(self.bucket, dp_path).get()
        content = dpjson['Body'].read().decode("utf-8")
        self.assertEquals(json.loads(content)['owner'], 'me')
        self.assertEquals(json.loads(content)['name'], 'my-datapackage')
        self.assertEqual(dpjson['ContentType'], self.params['content_type'])

        # Check csv content
        obj = s3.Object(self.bucket, csv_path).get()
        content = obj['Body'].read().decode("utf-8")
        expected_csv = 'Date,Name\r\n2001-02-03,Name\r\n'
        self.assertEquals(content, expected_csv)
        self.assertEqual(obj['ContentType'], self.params['content_type'])
