import json
import traceback
import requests
import boto3
import datetime
import os
import io
import sys
import time
import decimal
import base64
from pprint import pformat
import logging
from contextlib import redirect_stdout, redirect_stderr
import inspect
from requests import status_codes
from boto3.dynamodb.conditions import Key, Attr

formatter = logging.Formatter("%(levelname)s: %(message)s (%(filename)s:%(lineno)s)")
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.d = log.debug
log.i = log.info
log.w = log.warning
log.e = log.error
log.c = log.critical
log.x = log.exception

real_print = print
print = log.i

def pprint(x):
    log.i(pformat(x))

dynamodb = boto3.resource('dynamodb')
ses_client = boto3.client('ses')
sqs_client = boto3.client('sqs')
table = dynamodb.Table(os.environ['db_table'])
lambda_client = boto3.client('lambda')

assert os.environ['alert_email']
assert os.environ['alert_email_source']
assert os.environ['db_table']
assert os.environ['owm_appid']
assert os.environ['disable_email'] in ['true', 'false']
assert os.environ['log_events'] in ['all', 'errors']
assert os.environ['twilio_auth']
assert os.environ['twilio_sid']
assert os.environ['twilio_num']


def lambda_handler(event, context):
    aux_log_stream = io.StringIO()
    ch = logging.StreamHandler(aux_log_stream)
    ch.setFormatter(formatter)
    log.addHandler(ch)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    log.addHandler(ch)
    del ch

    def structured_return(statusCode, message, body = None):
        log.d(f"{inspect.currentframe().f_code.co_name}()")
        if not body: body = {}
        bodyReturn = {}
        awsReturn = {}
        if statusCode >= 400:
            bodyReturn['error'] = True
        else:
            bodyReturn['error'] = False
        bodyReturn['code'] = statusCode
        if statusCode in status_codes._codes:
            bodyReturn['status'] = status_codes._codes[statusCode][0]
        else:
            bodyReturn['status'] = 'Custom Exception'
        bodyReturn['message'] = message
        awsReturn['statusCode'] = statusCode
        # merge-in the body dict passed into the function
        bodyReturn = {**bodyReturn, **body}
        awsReturn['body'] = json.dumps(bodyReturn, default=json_default)
        awsReturn['headers'] =  { 'Access-Control-Allow-Origin': '*',
                                  'Cache-Control': 'no-store, must-revalidate'
                                }

        if os.environ['log_events'] == 'true':
            table.update_item(
                Key={ 'guid': 'event_log_' + context.aws_request_id },
                UpdateExpression = 'set invocation_result = :result',
                ExpressionAttributeValues = {':result': awsReturn}
            )

        return awsReturn


    
    context_dict = {
        'function_name': context.function_name,
        'function_version': context.function_version,
        'invoked_function_arn': context.invoked_function_arn,
        'memory_limit_in_mb': context.memory_limit_in_mb,
        'aws_request_id': context.aws_request_id,
        'log_group_name': context.log_group_name,
        'log_stream_name': context.log_stream_name
    }

    try:
        traceback_text = ''
            
            
        if os.environ['log_events'] == 'true':
            table.put_item(Item = {
                'guid': 'event_log_' + context.aws_request_id,
                'moment': str(datetime.datetime.utcnow()),
                'meta': 'event_log',
                'aws_request_id': context.aws_request_id,
                'event': json.loads(json.dumps(event, default=json_default), parse_float=decimal.Decimal), # passing through json because dynamo will crash on floats
                'context': context_dict,
                'ttl': int(time.time()) + (3600 * 24)
            })

        log.d(f'lambda function "{context.function_name}" has started')
        with redirect_stdout(aux_log_stream), redirect_stderr(aux_log_stream):

            ################################## < Not Boiler Plate > ##########################
            
            all_farms = table.query(
                IndexName='meta-guid-index',
                KeyConditionExpression = Key('meta').eq('farm_meta') & Key('guid').begins_with(f"farm_meta")
            )['Items']
            
            today_timestamp = int(time.time())
            today_date = time.strftime('%Y-%m-%d', time.gmtime(today_timestamp))
            today_datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(today_timestamp))
            
            yesterday_timestamp = today_timestamp - (60*60*24)
            yesterday_date = time.strftime('%Y-%m-%d', time.gmtime(yesterday_timestamp))
            yesterday_datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(yesterday_timestamp))
        
            for farm_conf in all_farms:
                farm_name = farm_conf['farm_name']
                
                yesterday_weather = requests.get(
                    'https://api.openweathermap.org/data/2.5/onecall/timemachine',
                    params = {
                        'appid': os.environ['owm_appid'],
                        'lat': farm_conf['latitude'],
                        'lon': farm_conf['longitude'],
                        'units': farm_conf['units'],
                        'dt': yesterday_timestamp
                    },
                    timeout = 30
                ).json()

                today_weather = requests.get(
                    'https://api.openweathermap.org/data/2.5/onecall/timemachine',
                    params = {
                        'appid': os.environ['owm_appid'],
                        'lat': farm_conf['latitude'],
                        'lon': farm_conf['longitude'],
                        'units': farm_conf['units'],
                        'dt': today_timestamp
                    },
                    timeout = 30
                ).json()

                forcast = requests.get(
                    'https://api.openweathermap.org/data/2.5/onecall',
                    params = {
                        'appid': os.environ['owm_appid'],
                        'lat': farm_conf['latitude'],
                        'lon': farm_conf['longitude'],
                        'units': farm_conf['units'],
                        'exclude': 'alerts,hourly,minutely'
                    },
                    timeout = 30
                ).json()

                last_precipitation = farm_conf['last_precipitation']

                precipitation_yesterday = False
                for hour in yesterday_weather['hourly']:
                    for weather in hour['weather']:
                        if weather['id'] <= 701:
                            last_precipitation = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(hour['dt']))
                            precipitation_yesterday = True

                precipitation_today = False
                for hour in today_weather['hourly']:
                    for weather in hour['weather']:
                        if weather['id'] <= 701:
                            last_precipitation = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(hour['dt']))
                            precipitation_today = True

                precipitation_forecasted = False
                for hour in forcast['daily']:
                    for weather in hour['weather']:
                        if weather['id'] <= 701:
                            precipitation_forecasted = time.strftime('%Y-%m-%d', time.gmtime(hour['dt']))


                if not 'Item' in table.get_item(Key = {'guid': f"weather_log-{farm_name}-{yesterday_date}"}):
                    table.put_item(
                        Item = {
                            'guid': f"weather_log-{farm_name}-{yesterday_date}",
                            'meta': 'weather_log',
                            'farm_name': 'english_orchards',
                            'date': yesterday_date,
                            'hourly': json.loads(json.dumps(yesterday_weather['hourly']), parse_float=decimal.Decimal)
                        }
                    )

                if farm_conf['pesticide_alerts']:
                    if last_precipitation > farm_conf['last_pesticide_alert']:
                        if not precipitation_forecasted:
                            requests.post (
                                f"https://api.twilio.com/2010-04-01/Accounts/{os.environ['twilio_sid']}/Messages.json",
                                data={
                                    'Body': f"Time to apply pesticide at {farm_conf['farm_name']}",
                                    'From': os.environ['twilio_num'],
                                    'To': farm_conf['alert_number']
                                },
                                auth = (os.environ['twilio_sid'], os.environ['twilio_auth'])
                            )

                table.update_item (
                    Key={ 'guid': farm_conf['guid'] },
                    UpdateExpression = (
                        'set last_pesticide_alert = :last_pesticide_alert'
                        ', last_precipitation = :last_precipitation'
                        ', last_update = :last_update'
                    ),
                    ExpressionAttributeValues = {
                        ':last_pesticide_alert': today_datetime,
                        ':last_precipitation': last_precipitation,
                        ':last_update': today_datetime,
                    }
                )


            ################################## < / Not Boiler Plate > #########################
        log.d(f'lambda function "{context.function_name}" has ended')
        
    except Exception:
        sys.stdout.flush()
        aux_log_stream.flush()

        traceback_text = traceback.format_exc()
        
        if 'disable_email' not in os.environ or os.environ['disable_email'] == 'false':
            ses_client.send_email(
                Destination={ 'ToAddresses': [ os.environ['alert_email'] ]},
                Message={
                    'Body': {
                        'Text': {
                            'Charset': "UTF-8",
                            'Data': (
                                f"error_log_{context.aws_request_id}:\n\n{aux_log_stream.getvalue()}\n{traceback_text}"
                            )
                        },
                    },
                    'Subject': {
                        'Charset': "UTF-8",
                        'Data': f'uncaught exception in {context.function_name}'
                    },
                },
                Source=os.environ['alert_email_source']
            )

        full_log = aux_log_stream.getvalue() + "\r\n" + traceback_text
        if len(full_log) > 150000: full_log = f"[tuncated]\r\n{full_log[-149900:]}"
        if os.environ['log_events'] in ['true', 'errors']:
            table.put_item(Item = {
                'code': 550,
                'meta': 'error_log',
                'guid': 'error_log_' + context.aws_request_id,
                'function_name': context.function_name,
                'log_stream_name': context.log_stream_name,
                'aws_request_id': context.aws_request_id,
                'moment': str(datetime.datetime.utcnow()),
                'ttl': int(time.time()) + (3600 * 24 * 90),
                'full_log': full_log,
                'event': json.loads(json.dumps(event, default=json_default), parse_float=decimal.Decimal), # passing through json because dynamo will crash on floats
            })

        if os.environ['log_events'] == 'true':
            table.update_item(
                Key={ 'guid': 'event_log_' + context.aws_request_id },
                UpdateExpression = 'set uncaught_exception = :e',
                ExpressionAttributeValues = {':e': 'error_log_' + context.aws_request_id}
            )

        return structured_return(550, 'eventID: ' + context.aws_request_id)
    finally:
        sys.stdout.flush()
        aux_log_stream.flush()
    
        full_log = aux_log_stream.getvalue()
        if traceback_text:
            full_log += '\r\n' + traceback_text
            real_print(traceback_text)
        if len(full_log) > 150000: full_log = f"[tuncated]\r\n{full_log[-149900:]}"
        if os.environ['log_events'] == 'true':

            table.update_item(
                Key={ 'guid': 'event_log_' + context.aws_request_id, },
                UpdateExpression = 'set full_log = :full_log',
                ExpressionAttributeValues = {':full_log': full_log}
            )
        real_print(f"\n\n\n\n<full_log>\n{full_log}\n</full_log>\n\n\n\n")



def json_default(obj):
    log.d(f"{inspect.currentframe().f_code.co_name}()")

    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode()
    raise TypeError



# {
  # "alert_number": "xxxxxxxx",
  # "farm_name": "my_orchards",
  # "guid": "farm_meta-my_orchards",
  # "last_pesticide_alert": "1970-01-01",
  # "last_precipitation": "1970-01-01",
  # "last_update": "1970-01-01",
  # "latitude": "12.5947",
  # "longitude": "-14.0012",
  # "meta": "farm_meta",
  # "pesticide_alerts": true,
  # "units": "imperial"
# }

