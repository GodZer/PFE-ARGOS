import json
import gzip
import base64
import os
import requests

def lambda_handler(event, context):

    api_url = os.environ['API_URL']
    print(f"Sending trail to {api_url}")
    
    cloudwatch_event = event['awslogs']['data']
    decode_base64 = base64.b64decode(cloudwatch_event)
    decompress_data = gzip.decompress(decode_base64)
    log_data = json.loads(decompress_data)
    #print(log_data)
    messages=[message["message"] for message in log_data["logEvents"]]
    print(messages)

    for msg in messages:
        requests.post(api_url, data=msg, headers={
            'Content-Type': 'application/json'
        })
        print("Trail forwarded successfully")
        
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': messages
    }
