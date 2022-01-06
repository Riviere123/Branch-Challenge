import json
import os
import boto3
from boto3.dynamodb.conditions import Key

REGION_NAME = os.getenv('REGION_NAME', 'us-east-1')
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME, endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
table = dynamodb.Table('Branch')


def get_branchId_by_billing(billingAccountNumber):
    '''
    returns the branchId of the provided billingAccountNumber
    '''
    try:
        response = table.query(
            IndexName='billingAccountNumber-index',
            KeyConditionExpression=Key('billingAccountNumber').eq(str(billingAccountNumber))
        )
        branchId = response["Items"][0]['branchId']

        return branchId
    except:
        return None

def delete_billing(billingAccountNumber):
    '''
    deletes the billing account using the billingAccountNumber
    '''
    try:
        branchId = get_branchId_by_billing(billingAccountNumber)
        table.delete_item(
            Key={
                'branchId': branchId,
                'billingAccountNumber': billingAccountNumber
            }
        )
        return f"billing account {billingAccountNumber} deleted."
    except:
        return f"billing account {billingAccountNumber} not found."

def append_service_account_number(branchId, billingAccountNumber, serviceAccountNumber):
    '''
    appends the serviceAccountNumber to the billing account
    '''
    try:
        response = table.update_item(
            Key={
                'branchId': branchId,
                'billingAccountNumber': billingAccountNumber
            },
            ConditionExpression="NOT contains(#SAN, :SAN)",
            UpdateExpression="SET #SAN = list_append(#SAN,:SAN)",
            ExpressionAttributeNames={
                "#SAN": "serviceAccountNumber"
            },
            ExpressionAttributeValues={
                ":SAN": [serviceAccountNumber]
            },
            ReturnValues='ALL_NEW'
            )
        return response['Attributes']
    except:
        return None

def lambda_handler(event, context):
    billingAccountNumber = event["pathParameters"]["billingAccountNumber"]

    if event['httpMethod'] == "GET":
        branchId = get_branchId_by_billing(billingAccountNumber)
        return({
            "statusCode": 200,
            "body": json.dumps(branchId)
        })
        
    elif event['httpMethod'] == "DELETE":
        message = delete_billing(billingAccountNumber)
        return({
            "statusCode": 200,
            "body": json.dumps(message)
        })
    elif event['httpMethod'] == "PUT":
        put_data = json.loads(event['body'])
        serviceAccountNumber = str(put_data['ServiceAccountNumber'])
        branchId = get_branchId_by_billing(billingAccountNumber)
        billingAccount = append_service_account_number(branchId, billingAccountNumber, serviceAccountNumber)
        return({
            "statusCode": 200,
            "body": json.dumps(billingAccount)
        })
