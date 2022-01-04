import json
import os
import boto3

REGION_NAME = os.getenv('REGION_NAME', 'us-east-1')
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME, endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
table = dynamodb.Table('Branch')

def create_account(branchId, crmId):
    account = {
            'branchId': branchId,
            'billingAccountNumber': 'null',
            'crmId': crmId
        }
    
    table.put_item(Item=account)
    return account

def lambda_handler(event, context):
    if event['httpMethod'] == "POST":
        post_data = json.loads(event['body'])
        branchId = str(post_data['branchId'])
        crmId = str(post_data['crmId'])
        account = create_account(branchId, crmId)
        return({
            "statusCode": 200,
            "body": json.dumps(account)
        })