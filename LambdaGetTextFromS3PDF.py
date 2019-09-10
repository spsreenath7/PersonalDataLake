import json
import urllib.parse
import boto3

print('Loading function')
print("boto3 version:"+boto3.__version__)

def lambda_handler(event, context):
    print("Triggered getTextFromS3PDF event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    dynamodb = boto3.resource('dynamodb')

    userTable = dynamodb.Table('PDS_UserProfile')
    userTabResponse = userTable.get_item(Key={'username': key.split('/')[0]})
    pdsuser = userTabResponse['Item']['userid']

    table = dynamodb.Table('PDS_Meta_RawZone')
    table.put_item(
        Item={
            'userid': pdsuser,
            'bucket': bucket,
            'resource': key,
            'catogery': 'finance',
            'format': 'pdf'
        }
    )
    print("Triggered Bucket: " +bucket)
    print("Triggered Name: " +key)    
    try:
        textract = boto3.client('textract')
        textract.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },FeatureTypes=["TABLES"],
        JobTag=key.split('/')[3] + '_Job',
        NotificationChannel={
            'RoleArn': 'arn:aws:iam::291941729863:role/AWSSNSFullAccessRole',
            'SNSTopicArn': 'arn:aws:sns:eu-west-1:291941729863:PDF_textract'
        })
        
        return 'Triggered PDF Processing for ' + key
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e