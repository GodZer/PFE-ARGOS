import json
import gzip
import base64

def lambda_handler(event, context):
    
    cloudwatch_event = event['awslogs']['data']
    decode_base64 = base64.b64decode(cloudwatch_event)
    decompress_data = gzip.decompress(decode_base64)
    log_data = json.loads(decompress_data)
    #print(log_data)
    messages=[message["message"] for message in log_data["logEvents"]]
    print(messages)
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': messages
    }
