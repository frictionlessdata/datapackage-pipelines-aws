import unittest

from datapackage_pipelines_aws import helpers

class TestToS3Proccessor(unittest.TestCase):
    def test_generate_path(self):
        inpath = 'datapackage.json'
        basepath = 'my/test/path'
        expected = 'my/test/path/datapackage.json'
        datapackage = {'name': 'my-package'}
        out = helpers.generate_path(inpath, basepath, datapackage)
        self.assertEquals(out, expected)

    def test_generate_path_with_formated_string(self):
        inpath = 'datapackage.json'
        basepath = 'my/test/path/{owner}/{name}/{version}'
        expected = 'my/test/path/me/my-package/latest/datapackage.json'
        datapackage = {'name': 'my-package', 'owner': 'me'}
        out = helpers.generate_path(inpath, basepath, datapackage)
        self.assertEquals(out, expected)

    def test_generate_path_errors_without_owner_in_datapackage(self):
        inpath = 'datapackage.json'
        basepath = 'my/test/path/{owner}/{name}/{version}'
        expected = 'my/test/path/me/my-package/latest/datapackage.json'
        datapackage = {'name': 'my-package',}
        with self.assertRaises(KeyError) as context:
            helpers.generate_path(inpath, basepath, datapackage)
