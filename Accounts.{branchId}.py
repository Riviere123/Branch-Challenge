import json
import os
import boto3
from boto3.dynamodb.conditions import Key

REGION_NAME = os.getenv('REGION_NAME', 'us-east-1')
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME, endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
table = dynamodb.Table('Branch')

def check_for_branchId(branchId):
    response = table.query(
            KeyConditionExpression=Key('branchId').eq(str(branchId))
        )
    items = response['Items']

    if len(items) <= 0:
        return None
    else:
        return response['Items']

def get_account_by_branchId(branchId):
    items = check_for_branchId(branchId)
    if items:
        accountNumbers = []
        payload = None
        for item in items:
            if item['billingAccountNumber'] != "null":
                accountNumbers.append(item['billingAccountNumber'])
            else:
                payload = item
        payload['billingAccountNumber'] = accountNumbers
        return payload
    else:
        return "branchId not in use."


def add_billing_account_number(branchId, billing_account_number):
    if check_for_branchId(branchId):
        data = {
            'branchId':branchId,
            'billingAccountNumber': billing_account_number
        }
        table.put_item(Item=data)
        account = get_account_by_branchId(branchId)
        return account
    else:
        return "branchId not in use."

def append_account_data(branchId, key, value):
    table.update_item(
    Key={
        'branchId':branchId,
        'billingAccountNumber':"null"
        },
        UpdateExpression=f'SET {key} = :val1',
        ExpressionAttributeValues={
            ':val1': value
        }
    )
    account = get_account_by_branchId(branchId)
    return account

def delete_account(branchId):
    account = get_account_by_branchId(branchId)
    for billing in account['billingAccountNumber']:
        table.delete_item(
            Key={
                'branchId': branchId,
                'billingAccountNumber': billing
            }
        )
    table.delete_item(
        Key={
            'branchId': branchId,
            'billingAccountNumber': "null"
        }
    )

def lambda_handler(event, context):
    if event['httpMethod'] == "GET":
        branchId = event["pathParameters"]["branchId"]
        account = get_account_by_branchId(branchId)
        return({
            "statusCode": 200,
            "body": json.dumps(account)
        })

        
    elif event['httpMethod'] == "POST":
        branchId = event["pathParameters"]["branchId"]
        post_data = json.loads(event['body'])
        billing_account_number = str(post_data['billingAccountNumber'])
        account = add_billing_account_number(branchId, billing_account_number)

        return({
            "statusCode": 200,
            "body": json.dumps(account)
        })
        
    elif event['httpMethod'] == "PUT":
        branchId = event["pathParameters"]["branchId"]
        put_data = json.loads(event['body'])
        key = list(put_data.keys())[0]
        value = put_data[key]
        account = append_account_data(branchId, key, value)
        return({
            "statusCode": 200,
            "body": json.dumps(account)
        })
    
    elif event['httpMethod'] == "DELETE":
        branchId = event["pathParameters"]["branchId"]
        delete_account(branchId)
        return({
            "statusCode": 200,
            "body": json.dumps("account deleted")
        })