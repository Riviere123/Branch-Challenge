import json
import os
import boto3
from boto3.dynamodb.conditions import Key

REGION_NAME = os.getenv('REGION_NAME', 'us-east-1')
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME, endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
table = dynamodb.Table('Branch')


def get_branchId_by_billing(billing_account_number):
    '''
    returns the branchId of the provided billingAccountNumber
    '''
    try:
        if billing_account_number == "null":
            return None
        response = table.query(
            IndexName='billingAccountNumber-index',
            KeyConditionExpression=Key('billingAccountNumber').eq(str(billing_account_number))
        )
        branchId = response["Items"][0]['branchId']

        return branchId
    except:
        return None

def delete_billing(billing_account_number):
    '''
    deletes the billing account using the billing_account_number
    '''
    try:
        branchId = get_branchId_by_billing(billing_account_number)
        table.delete_item(
            Key={
                'branchId': branchId,
                'billingAccountNumber': billing_account_number
            }
        )
        return f"billing account {billing_account_number} deleted."
    except:
        return f"billing account {billing_account_number} not found."

def append_service_account_number(branchId, billing_account_number, service_account_number):
    '''
    appends the service_account_number to the billing account
    '''
    try:
        response = table.update_item(
            Key={
                'branchId': branchId,
                'billingAccountNumber': billing_account_number
            },
            ConditionExpression="NOT contains(#SAN, :CON)",
            UpdateExpression="SET #SAN = list_append(#SAN,:SAN)",
            ExpressionAttributeNames={
                "#SAN": "serviceAccountNumber"
            },
            ExpressionAttributeValues={
                ":SAN": [service_account_number],
                ":CON": service_account_number
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
