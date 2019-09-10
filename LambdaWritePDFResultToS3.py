import json
import boto3
import os
from trp import Document

def getJobResults(jobId):

    pages = []

    textract = boto3.client('textract')
    response = textract.get_document_analysis(JobId=jobId,MaxResults=250)
    
    pages.append(response)

    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):

        response = textract.get_document_analysis(JobId=jobId,MaxResults=250,NextToken=nextToken)

        pages.append(response)
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages

def lambda_handler(event, context):
    notificationMessage = json.loads(json.dumps(event))['Records'][0]['Sns']['Message']
    print('Loading function')
    print("boto3 version:"+boto3.__version__)
    pdfTextExtractionStatus = json.loads(notificationMessage)['Status']
    pdfTextExtractionJobTag = json.loads(notificationMessage)['JobTag']
    pdfTextExtractionJobId = json.loads(notificationMessage)['JobId']
    pdfTextExtractionDocumentLocation = json.loads(notificationMessage)['DocumentLocation']
    
    pdfTextExtractionS3ObjectName = json.loads(json.dumps(pdfTextExtractionDocumentLocation))['S3ObjectName']
    pdfTextExtractionS3Bucket = json.loads(json.dumps(pdfTextExtractionDocumentLocation))['S3Bucket']
    
    print(pdfTextExtractionJobTag + ' : ' + pdfTextExtractionStatus)
    
    pdfText = ''
    
    if(pdfTextExtractionStatus == 'SUCCEEDED'):
        response = getJobResults(pdfTextExtractionJobId)

    doc = Document(response)

    warning = ""
    tabdata = []
    matstertext= ""
    for page in doc.pages:
         # Print tables
        for table in page.tables:
            for r, row in enumerate(table.rows):
                itemName  = ""
                tabrow = []
                for c, cell in enumerate(row.cells):
                    tabrow.append(cell.text)
                    matstertext+=cell.text
                    matstertext+=" ,"
                tabdata.append(tabrow)
                matstertext+="\n"

    s3 = boto3.client('s3')
    print("====csv contents ==========")
    print(matstertext)
    outputTextFileName = pdfTextExtractionS3ObjectName.split('.')[0] + '.csv'
    s3.put_object(Body=matstertext, Bucket=pdfTextExtractionS3Bucket, Key=outputTextFileName)
    print("====csv Done ==========")