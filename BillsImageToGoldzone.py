import json
import boto3
import os
import urllib.parse
import uuid

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Amazon Textract client
textract = boto3.client('textract')

def getTextractData(bucketName, documentKey):
    print('Loading getTextractData')
    # Call Amazon Textract
    response = textract.detect_document_text(
        Document={
            'S3Object': {
                'Bucket': bucketName,
                'Name': documentKey
            }
        })
        
    detectedText = ''

    # Print detected text
    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            detectedText += item['Text'] + '\n'
            
    return detectedText
    
def writeTextractToS3File(textractData, bucketName, createdS3Document):
    print('Loading writeTextractToS3File')
    generateFilePath = os.path.splitext(createdS3Document)[0] + '.txt'
    s3.put_object(Body=textractData, Bucket=bucketName, Key=generateFilePath)
    print('Generated ' + generateFilePath)

def processAldi(values, pdsuser):
    recList = []
    i = 1
    while (i < len(values)-3):
        record = {}
        record['purchaseid'] = uuid.uuid4().hex[:8]
        record['user'] = pdsuser
        record['purchasedate'] = '01/01/2019'
        record['store'] = 'Aldi'
        record['currency'] = 'euro'
        words = []
        words = values[i].split(' ')
        words = words[1:]
        record['itemname'] = ' '.join(words)
        record['price'] = values[i+1][:4]
        recList.append(record)
        print(record)
        i = i + 2

    return recList
    
def processTesco(values, pdsuser):
    recList = []
    i = 0
    while (i < len(values)-4):
        record = {}
        record['purchaseid'] = uuid.uuid4().hex[:8]
        record['user'] = pdsuser
        record['purchasedate'] = '01/01/2019'
        record['store'] = 'Tesco'
        record['currency'] = 'euro'
        record['itemname'] = values[i]
        record['price'] = values[i+1][3:]
        recList.append(record)
        print(record)
        i = i + 2

    return recList
    

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("Bucket : "+bucket)
    print("Key : "+key)
    
    userTable = dynamodb.Table('PDS_UserProfile')
    userTabResponse = userTable.get_item(Key={'username': key.split('/')[0]})
    pdsuser = userTabResponse['Item']['userid']
    
    table = dynamodb.Table('PDS_Meta_RawZone')
    table.put_item(
        Item={
            'userid': pdsuser,
            'bucket': bucket,
            'resource': key,
            'catogery': 'shopping',
            'format': 'image'
        }
    )
    
    try:
        detectedText = getTextractData(bucket, key)
        values = detectedText.split('\n')
        writeTextractToS3File(detectedText, bucket, key)
        if(key.split('/')[3][:3] == 'ALD'):
            records= processAldi(values, pdsuser)
        if(key.split('/')[3][:3] == 'TES'):        
            records= processTesco(values, pdsuser)
        purchaseTable = dynamodb.Table('PDS_GZ_ShoppingData')
        with purchaseTable.batch_writer() as batch:
            for item in records:
                batch.put_item(Item=item)
        print(records)
        return 'Processing Done!'

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e