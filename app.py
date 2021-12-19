from __future__ import absolute_import
import typing
import sys
import json
import boto3
import hashlib
from awsglue.utils import getResolvedOptions
import requests
from opensearchpy import OpenSearch
from botocore.exceptions import ClientError


args = getResolvedOptions(sys.argv, ["S3_INPUT_PATH","ES_HOST","ES_PORT","ES_USERNAME","ES_SECRET_KEY"])

INDEX_NAME = "wikidata"
src_path = args["S3_INPUT_PATH"]
s3 = boto3.resource('s3')
s3_bucket_index = src_path.replace("s3://","").find("/")
s3_bucket = src_path[5:s3_bucket_index+5]
s3_key = src_path[s3_bucket_index+6:]
bucket = s3.Bucket(s3_bucket)
objs = [(x.bucket_name,x.key) for x in list(bucket.objects.filter(Prefix=s3_key)) if x.key.endswith(".wikitext")]


def get_secret_value(name, version=None):
    """Gets the value of a secret.

    Version (if defined) is used to retrieve a particular version of
    the secret.

    """
    secrets_client = boto3.client("secretsmanager")
    kwargs = {'SecretId': name}
    if version is not None:
        kwargs['VersionStage'] = version
    response = secrets_client.get_secret_value(**kwargs)
    return response




def post_to_es(document):
    host = args["ES_HOST"]
    port = args["ES_PORT"]
    username = args["ES_USERNAME"]
    password = get_secret_value(args["ES_SECRET_KEY"])
    
    auth = (username, password)
    
    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        http_auth = auth,
        # client_cert = client_cert_path,
        # client_key = client_key_path,
        use_ssl = False,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False,
        #ca_certs = ca_certs_path
    )
    
    response = client.index(
        index = INDEX_NAME,
        body = document,
        refresh = True
        )

def file_parsing(bucket, file_path):
    parseddict = []
    title = file_path[file_path.rfind("/")+1:]
    hsh = file_path.replace("/"+title,"")
    data = s3.Object(bucket_name=file[0], key=file[1]).get()["Body"].read()
 
    groups = data.decode().split("\r\n\r\n\r\n")
    del data
    for group in groups[1:]:
        author = group[:group.find("\n")]
        paragraph = group[group.find("\n")+1:]
        parseddict.append({"title":title,"author":author,"paragraph":paragraph, "hash":hsh})
    return parseddict

for file in objs:
    parseddict = file_parsing(file[0], file[1])
    print(parseddict)
