import awscli
import boto3

#def upload_file(file_name, bucket, object_name=None):
#available buckets are ['bwp-gms-mikedey-python-modules', 'bwp-gms-mikedey-rds-trigger-bucket', 'bwp-gms-mikedey-special-chars']

s3=boto3.client('s3')
s3.upload_file('test2.py','bwp-gms-mikedey-special-chars','test2-in-s3.txt')


