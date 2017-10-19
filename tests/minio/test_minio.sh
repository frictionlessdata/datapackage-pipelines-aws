#!/usr/bin/env bash

export AWS_ACCESS_KEY_ID="HJW6280KHBS2Y105STGG"
export AWS_SECRET_ACCESS_KEY="rsAuRptRwTTuSocsdBCRHIToldPkPefpb2Vl/ybG"
export S3_ENDPOINT_URL="http://localhost:9000"

if [ ! -d tests/minio ]; then
    echo "This script should run from the project root directory"
    exit 1
fi

echo "noise:
  pipeline:
  - run: noise
  - run: aws.dump.to_s3
    parameters:
      bucket: noise" > tests/minio/pipeline-spec.yaml

# start the pipeline before starting the server to check the retry mechanism
dpp run ./tests/minio/noise &

sleep 2

docker run -p9000:9000 \
    --rm \
    --name datapackage-pipelines-aws-minio-test \
    "-eMINIO_ACCESS_KEY=${AWS_ACCESS_KEY_ID}" \
    "-eMINIO_SECRET_KEY=${AWS_SECRET_ACCESS_KEY}" \
    minio/minio server /data &

sleep 10

rm tests/minio/pipeline-spec.yaml

echo
echo
echo " > Review the generated data in http://localhost:9000"
echo " > Stop the minio server when done - docker rm --force datapackage-pipelines-aws-minio-test"
echo
