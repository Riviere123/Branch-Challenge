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
    response = table.query(
        IndexName='billingAccountNumber-index',
        KeyConditionExpression=Key('billingAccountNumber').eq(str(billingAccountNumber))
    )
    branchId = response["Items"][0]['branchId']

    return branchId

def delete_billing(billingAccountNumber):
    '''
    deletes the billing account with the provided number
    '''
    branchId = get_branchId_by_billing(billingAccountNumber)
    table.delete_item(
        Key={
            'branchId': branchId,
            'billingAccountNumber': billingAccountNumber
        }
    )

def lambda_handler(event, context):
    billingAccountNumber = event["pathParameters"]["billingAccountNumber"]

    if event['httpMethod'] == "GET":
        branchId = get_branchId_by_billing(billingAccountNumber)
        return({
            "statusCode": 200,
            "body": json.dumps({"branchId":branchId})
        })
        
    elif event['httpMethod'] == "DELETE":
        delete_billing(billingAccountNumber)
        return({
            "statusCode": 200,
            "body": json.dumps("billing account deleted")
        })
        
        
