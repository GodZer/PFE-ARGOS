import json
import hashlib
import sys
import boto3
import os

firehose = boto3.client('firehose')
sagemaker = boto3.client('sagemaker-runtime')

columns = [
    "verb",
    "username",
    "groups",
    "userAgent",
    "sourceIPs",
    "resource",
    "subresource",
    "name",
    "apiGroup",
    "namespace",
    "impersonatedUser",
]


def handler(event, context):

    #print(event)
    body = event["body"]
    trail = json.loads(body)
    #print(trail)
    array = createArrayfromjson(trail)
    dictionary = {}

    for i in range(len(columns)):
        dictionary[columns[i]] = array[i]

    dictionary["encodeur"] = hash_arrayString(array)
    solutionJson = json.dumps(dictionary)
    
    object2vec_request={
        "instances": [
            {
                "in0": dictionary["encodeur"]
            }
        ]
    }

    try:
        object2vec_request={
            "instances": [
                {
                    "in0": dictionary["encodeur"]
                }
            ]
        }
        inference_endpoint_name=get_inference_endpoint_name(dictionary["username"])
        object2vec_response=sagemaker.invoke_endpoint(
            EndpointName=inference_endpoint_name,
            TargetVariant="o2v",
            Body=json.dumps(object2vec_request)
        )
        object2vec_response=json.loads(object2vec_response["Body"].read().decode())
        rcf_request={
            "instances": [
                {
                    "features": object2vec_response["predictions"][0]["embeddings"]
                }
            ]
        }
        rcf_reponse=sagemaker.invoke_endpoint(
            EndpointName=inference_endpoint_name,
            TargetVariant="rcf",
            Body=json.dumps(rcf_request),
            ContentType="application/json"
        )
        print(rcf_reponse["Body"].read().decode())
    except Exception as e:
        print(f"Error invoking Object2Vec: {e}")
    
    firehose.put_record(
        DeliveryStreamName=os.environ["FIREHOSE_DELIVERY_STREAM_NAME"],
        Record={
            'Data': solutionJson
        }
    )

    return {
        "statusCode": 200,
        "body": "Ok"
    }

def sanitize(partition_name):
    sanitized_string=map(lambda c: c if c.isalnum() else "-", partition_name)
    sanitized_string="".join(sanitized_string)
    return sanitized_string[:40]

def get_inference_endpoint_name(username):
    sanitized_username=sanitize(username)
    return sanitized_username+"-endpoint"


def loadJson(jsonlog):
    keys = []
    values = []
    for key, value in jsonlog.items():
        if type(value) == dict:
            for key2, value2 in value.items():
                keys.append(key2)
                values.append(value2)
        else:
            keys.append(key)
            values.append(value)
    return keys, values


def createArrayfromjson(jsondict):

    logkey, logvalue = loadJson(jsondict)
    final_array = []

    if "groups" in logkey:
        logvalue[logkey.index("groups")] = "".join(logvalue[logkey.index("groups")])
    if "sourceIPs" in logkey:
        logvalue[logkey.index("sourceIPs")] = logvalue[logkey.index("sourceIPs")][0]

    for elem in columns:
        if elem in logkey:
            final_array.append(logvalue[logkey.index(elem)])
        else:
            final_array.append("missing")

    return final_array


def hash_string(string):
    return hash(string)&0x1fffff


def hash_arrayString(array):
    return [hash_string(elem) for elem in array]
