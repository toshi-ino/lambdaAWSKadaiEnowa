

from __future__ import print_function
import boto3
from boto3.dynamodb.conditions import Key
import datetime
import json
import traceback
import os
from decimal import Decimal
import urllib.parse

#-----Dynamo Info change here------
TABLE_NAME = os.environ.get('TABLE_NAME', "default")
DDB_PRIMARY_KEY = "deviceid"
DDB_SORT_KEY = "timestamp"
#-----Dynamo Info change here------

dynamodb = boto3.resource('dynamodb')
table  = dynamodb.Table(TABLE_NAME)

#------------------------------------------------------------------------
def dynamoQuery(deviceid, requestTime_start_str, requestTime_end_str):
    print("dynamoQuery start")

    valList = []
    res = table.query(
        # KeyConditionExpression=
        #     Key(DDB_PRIMARY_KEY).eq(deviceid) &
        #     Key(DDB_SORT_KEY).lt(requestTime),
        #     ScanIndexForward = False,
        #     Limit = 60*int(term)
        # )
        KeyConditionExpression=
            Key(DDB_PRIMARY_KEY).eq(deviceid) &
            Key(DDB_SORT_KEY).between(requestTime_start_str, requestTime_end_str),
            ScanIndexForward = False,
        )

    for row in res['Items']:
        # val = row['TEMPERATURE']
        # itemDict = {
        #     "timestamp":row['timestamp'],
        #     "value":int(val)
        # }
        
        val1 = row['TEMPERATURE']
        val2 = row['HUMIDITY']
        val3 = row['CO2']
        val4 = row['pF']

        itemDict = {
            "timestamp":row['timestamp'],
            "temp":int(val1),
            "humi":int(val2),
            "CO2":int(val3),
            "pF":int(val4)
        }
        valList.append(itemDict)

    return valList

#------------------------------------------------------------------------
# call by Lambda here.
#  Event structure : API-Gateway Lambda proxy post
#------------------------------------------------------------------------
def lambda_handler(event, context):
    #Lambda Proxy response back template
    HttpRes = {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin" : "*"},
        "body": "",
        "isBase64Encoded": False
    }

    try:
        print("lambda_handler start")
        print(json.dumps(event))
        
        
        param = urllib.parse.parse_qs(event['body'])
        sendBaseTime = param['sendBaseTime'][0]
        sendTimeEnd = param['sendTimeEnd'][0]
        # term = param['sendTerm'][0]
        
        print("sendBaseTime")
        print(sendBaseTime)
        print("sendTimeEnd")
        print(sendTimeEnd)

        # get Parameters
        pathParameters = event.get('pathParameters')
        deviceid = pathParameters["deviceid"]
        
        #Calculate Time
        date_format='%Y-%m-%dT%H:%M:%S'
        # timeDeltaHour=datetime.timedelta(hours=int(term))
        # timeDeltaMinute=datetime.timedelta(hours=int(1))
        # requestTime = sendBaseTime
        # requestTime_end_obj = datetime.datetime.strptime(requestTime, date_format)
        # requestTime_start_obj = requestTime_end_obj - timeDeltaHour - timeDeltaMinute
        # requestTime_end_str = requestTime_end_obj.strftime(date_format)
        # requestTime_start_str = requestTime_start_obj.strftime(date_format)
        requestTime_end_obj = datetime.datetime.strptime(sendTimeEnd, date_format)
        requestTime_start_obj = datetime.datetime.strptime(sendBaseTime, date_format)
        
        print("requestTime_end_obj")
        print(requestTime_end_obj)
        print("requestTime_start_obj")
        print(requestTime_start_obj)
        
        requestTime_end_str = requestTime_end_obj.strftime(date_format)
        requestTime_start_str = requestTime_start_obj.strftime(date_format)

        resItemDict = { deviceid : ""}
        # resItemDict[deviceid] = dynamoQuery(deviceid, requestTime, term)
        resItemDict[deviceid] = dynamoQuery(deviceid, requestTime_start_str, requestTime_end_str)

        HttpRes['body'] = json.dumps(resItemDict)

    except Exception as e:
        print(traceback.format_exc())
        HttpRes["statusCode"] = 500
        HttpRes["body"] = "Lambda error. check lambda log"

    # print(sendBaseTime)
    # print(term)
    # print("response:{}".format(json.dumps(HttpRes)))
    return HttpRes
