import json
import os
import boto3
from boto3.dynamodb.conditions import Key

REGION_NAME = os.getenv('REGION_NAME', 'us-east-1')
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME, endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
table = dynamodb.Table('Branch')

def check_for_branchId(branchId):
    '''
    Checks the table for any entries that have the given branchId.
    returns the entries if it's found, else returns none
    '''
    response = table.query(
            KeyConditionExpression=Key('branchId').eq(str(branchId))
        )
    items = response['Items']

    if len(items) <= 0:
        return None
    else:
        return response['Items']

def get_account_by_branchId(branchId):
    '''
    first checks if the branchId is in use. 
    if entries are found formats multiple entries into one
    by combining all the billing account numbers into the master account.
    NOTE:It is not possible to create a billing account without already having a main account.
    So account will never = None
    '''
    items = check_for_branchId(branchId)
    if items:
        accountNumbers = []
        account = None
        for item in items:
            if item['billingAccountNumber'] != "null":
                accountNumbers.append(item['billingAccountNumber'])
            else:
                account = item
        account['billingAccountNumber'] = accountNumbers
        return account
    else:
        return "branchId not in use."


def add_billing_account_number(branchId, billing_account_number):
    '''
    first checks if the branchId is in use. 
    if so makes a new billingAccountNumber entry in the table.
    '''
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
    '''
    first checks if the branchId is in use. 
    if so appends data to the main account
    '''
    if check_for_branchId(branchId):
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
    else:
        return "branchId not in use."

def delete_account(branchId):
    '''
    deletes the account and all billing account entries sharing the same branchId
    '''
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