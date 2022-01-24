import json
import hashlib
import boto3
import os

firehose = boto3.client('firehose')

columns = [
    "verb",
    "username",
    "groups",
    "userAgent",
    "sourceIPs",
    "resource",
    "subresource",
    "name",
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
    #return int.from_bytes(hashlib.sha256(str.encode(string.upper())).digest()[:4], "little")
    return hash(string)&0x1fffff


def hash_arrayString(array):
    return [hash_string(elem) for elem in array]
