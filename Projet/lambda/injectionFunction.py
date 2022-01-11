import json
import hashlib


def handler(event, context):

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

    def loadJson(jsonlog):
        keys = []
        values = []
        dic = json.dumps(jsonlog)
        jsonloaded = json.loads(dic)
        for key, value in jsonloaded.items():
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

        if "groups" in logkey:
            logvalue[logkey.index("groups")] = "".join(logvalue[logkey.index("groups")])
        if "sourceIPs" in logkey:
            logvalue[logkey.index("sourceIPs")] = logvalue[logkey.index("sourceIPs")][0]

        final_array = []

        for elem in columns:
            if elem in logkey:
                final_array.append(logvalue[logkey.index(elem)])
            else:
                final_array.append("missing")

        return final_array

    def hash_string(string):
        return int.from_bytes(hashlib.sha256(str.encode(string)).digest()[:4], "little")

    def hash_arrayString(array):
        return [hash_string(elem) for elem in array]

    array = createArrayfromjson(event)
    dictionary = {
        "trail": array,
        "encodeur": hash_arrayString(array),
    }
    solutionJson = json.dumps(dictionary)
    print(type(solutionJson))
    return solutionJson
