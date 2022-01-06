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
    returns the entries if it's found, else returns None
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
    finds and formats the branch account then returns the account object if it exists else returns None
    NOTE:It is not possible to create a billing account without already having a master account.
    So main_account will never = None. The master accounts billingAccountNumber is set to "null" on creation.
    '''
    accounts = check_for_branchId(branchId)
    if accounts:
        billing_accounts = {}
        main_account = None
        for account in accounts:
            if account['billingAccountNumber'] != "null":
                billing_accounts[account['billingAccountNumber']] = {"serviceAccountNumber":account['serviceAccountNumber']}
            else:
                main_account = account
        if main_account == None:
            return "branchId not in use."
        main_account['billingAccountNumber'] = billing_accounts
        return main_account
    else:
        return None


def add_billing_account_number(branchId, billing_account_number):
    '''
    first checks if the branchId is in use. if so makes a new billingAccountNumber entry in the table.
    '''
    if check_for_branchId(branchId):
        data = {
            'branchId':branchId,
            'billingAccountNumber': billing_account_number,
            'serviceAccountNumber': []
        }
        table.put_item(Item=data)
        account = get_account_by_branchId(branchId)
        return account
    else:
        return None

def append_account_data(branchId, key, value):
    '''
     Keyword arguments:
     Key - the table key you wish to set
     value - the value to set it to
    first checks if the branchId is in use. if so appends data to the main account
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
        #NOTE:Could have done a return value from update table but it would not inlcude the account numbers.
        #Unfortunately this function does an update and a query. If we don't need to return the account we could drop
        #the second query.
        account = get_account_by_branchId(branchId)
        return account
    else:
        return None

def delete_account(branchId):
    '''
    deletes the account and all billing account entries sharing the same branchId
    '''
    try:
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
        return f"account {branchId} deleted"
    except:
        return f"account {branchId}  not found"


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
        message = delete_account(branchId)
        return({
            "statusCode": 200,
            "body": json.dumps(message)
        })