import boto3
import time
import csv
import uuid
import urllib.parse

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("Bucket : "+bucket)
    print("Key : "+key)

    # Document
    actuals3BucketName = "pdsrzmytxtrcttest"
    s3BucketName = "mytxtrcttest"
    documentName = "junestmt.csv"

    lookUpTable = dynamodb.Table('PDs-Meta-Lookup')
    userTable = dynamodb.Table('PDS_UserProfile')
    
    userTable = dynamodb.Table('PDS_UserProfile')
    userTabResponse = userTable.get_item(Key={'username': key.split('/')[0]})
    pdsuser = userTabResponse['Item']['userid']

    object = s3.get_object(Bucket = bucket ,Key = key )
    print("=======================================================")
    rows = object['Body'].read().split(b'\n')
    header = rows[0].split(b'  ,')
    headervals = []
    for j in range(len(header)-1):
        print(header[j])
        response = lookUpTable.get_item(Key={'keyword': header[j].decode('ASCII').rstrip()})
        headervals.append(response['Item']['value'])
    recList = list()
    headerdone = False
    for row in rows:
        values = row.split(b'  ,')
        if(len(values)>1 and headerdone):
            record = {}
            record['transid'] = uuid.uuid4().hex[:8]
            record['user'] = pdsuser        
            for i in range(len(values)-1):
                print(headervals[i]+" : value : "+values[i].decode('ASCII'))
                if(headervals[i] != "notused"):
                    record[headervals[i]] = values[i].decode('ASCII').rstrip()
            print(record)
            recList.append(record)
        headerdone = True
    print("=======================================================")

    financeTable = dynamodb.Table('PDS_GZ_FinanceData')
    with financeTable.batch_writer() as batch:
        for item in recList:
            batch.put_item(Item=item)
    print(recList)