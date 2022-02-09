import json
import urllib.parse
import boto3
from datetime import datetime

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        timestamp = int(datetime.timestamp(datetime.now()))        
        content_object = s3.get_object(Bucket=bucket, Key=key)
        file_content = content_object['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)				
    
        return put_record(timestamp, json_content['temp'])        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

def put_record(timestamp, temp):
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('registros-temperatura-juan')
    response = table.put_item(
       Item={
            'PK': 'TEMP#SENSOR1',
            'SK': 'SENSOR#' + str(timestamp),
            'timestamp': timestamp,
            'temp': temp
            }
    )
    return response