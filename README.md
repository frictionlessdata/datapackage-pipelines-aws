# AWS Extensions for datapackage-pipelines

[![Build Status](https://travis-ci.org/frictionlessdata/datapackage-pipelines-aws.svg?branch=master)](https://travis-ci.org/frictionlessdata/datapackage-pipelines-aws)

## Install

```
# clone the repo and install it wit pip

git clone https://github.com/frictionlessdata/datapackage-pipelines-aws.git
pip install -e .
```

## Usage

You can use datapackage-pipelines-aws as a plugin for [dpp](https://github.com/frictionlessdata/datapackage-pipelines#datapackage-pipelines). In pipeline-spec.yaml it will look like this

```yaml
  ...
  - run: aws.dump.to_s3
```

You will need AWS credentials to be set up. See [the guide to set up the credentials](http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)

### dump.to_s3

Saves the DataPackage to AWS S3.

_Parameters:_

* `bucket` - Name of the bucket where DataPackage will be stored (should already be created!)
* `acl` - ACL to provide the uploaded files. Default is 'public-read' (see [boto3 docs](http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.put_object) for more info).
* `path` - Path (key/prefix) to the DataPackage. May contain format string available for `datapackage.json` Eg: `my/example/path/{owner}/{name}/{version}`
* `content_type` - content type to use when storing files in S3. Defaults to text/plain (usual S3 default is binary/octet-stream but we prefer text/plain).

_Example:_

```yaml
datahub:
  title: datahub-to-s3
  pipeline:
    -
      run: load_metadata
      parameters:
        url: http://example.com/my-datapackage/datapackage.json
    -
      run: load_resource
      parameters:
        url: http://example.com/my-datapackage/datapackage.json
        resource: my-resource
    -
      run: aws.dump.to_s3
      parameters:
        bucket: my.bucket.name
        path: path/{owner}/{name}/{version}
    -
      run: aws.dump.to_s3
      parameters:
        bucket: my.another.bucket
        path: another/path/{version}
        acl: private
```

Executing pipeline above will save DataPackage in the following directories on S3:
* my.bucket.name/path/my-name/py-package-name/latest/...
* my.bucket.name/another/path/latest/...
