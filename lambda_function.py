#This is an SNS event listener for AWS Lambda
#It runs in the standard python environment, so no venv needed
#Just set the SCALYR_API_KEY in lambda with a dataset.com API key, write permissions needed. 

import json
import os
import urllib.request
import urllib.error
import uuid
import time
from datetime import datetime

 

def get_current_timestamp_ns():
    """Returns the current time in nanoseconds since the UNIX epoch."""
    return int(time.time() * 1e9)

def iso_to_ns(timestamp):
    """Convert ISO 8601 timestamp to nanoseconds since the UNIX epoch."""
    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    return int(dt.timestamp() * 1e9)

 

def send_request_with_retry(req):
    max_retries = 3
    backoff_factor = 1.5
    wait_time = 1   
    for attempt in range(max_retries):

        try:
            with urllib.request.urlopen(req) as response:
                response_body = response.read()
                response_data = json.loads(response_body.decode('utf-8'))
                print(f"API Response on attempt {attempt+1}: {response_data}")  # Debugging API response
                if response.status == 200:
                    return response_data
                else:
                    print(f"Request failed with status {response.status}: {response_body.decode('utf-8')}")

        except urllib.error.HTTPError as e:
            error_body = e.read().decode()
            print(f"HTTP Error on attempt {attempt+1}: {e.code} {e.reason}, Body: {error_body}")
            
        except urllib.error.URLError as e:
            print(f"URL Error on attempt {attempt+1}: {e.reason}")
        time.sleep(wait_time)
        wait_time *= backoff_factor

    raise Exception("Failed to send data after several retries")


def lambda_handler(event, context):
    api_key = os.environ['SCALYR_API_KEY']
    endpoint_url = https://app.scalyr.com/api/addEvents

    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'server-host': 'AWS_SNS' #hostname1
    }

    session_guid = str(uuid.uuid4())
    events_data = {
        "session": session_guid,
        "sessionInfo": {
            "serverHost": "AWS_SNS", #hostname2
            "logfile": "lambda_logs",
            "parser": "lambda-parser"
        },
        "events": []
    }

    try:

        for record in event['Records']:
            sns_message = json.loads(record['Sns']['Message'])
            print(f"Received raw SNS message: {sns_message}")  # Debugging raw message

            # Extract timestamp and convert it
            raw_timestamp = sns_message.get("mail", {}).get("timestamp") or sns_message.get("open", {}).get("timestamp")
            timestamp = iso_to_ns(raw_timestamp) if raw_timestamp else get_current_timestamp_ns()


            # Include entire message in attributes
            event_payload = {
                "ts": timestamp,
                "attrs": sns_message
            }

            events_data["events"].append(event_payload)

       
        if events_data["events"]:
            json_data = json.dumps(events_data)
            print(f"Formatted events data to send: {json_data}")  # Debugging  
            req = urllib.request.Request(endpoint_url, data=json_data.encode('utf-8'), headers=headers, method='POST')
            response = send_request_with_retry(req)
            print(f"Final response from Scalyr: {response}")  # Debugging final response

        else:

            print("No events to send.")

 

    except Exception as e:

        print(f"An error occurred: {str(e)}")
        raise e

    return {

        'statusCode': 200,
        'body': json.dumps('Successfully processed SNS message.')

    }
