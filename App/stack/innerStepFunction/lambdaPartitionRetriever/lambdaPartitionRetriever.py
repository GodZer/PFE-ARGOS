import boto3
import os

glue=boto3.client("glue")
database_name=os.environ["DATABASE_NAME"]
table_name=os.environ["TABLE_NAME"]

def sanitize(partition_name):
    sanitized_string=map(lambda c: c if c.isalnum() else "-", partition_name)
    sanitized_string="".join(sanitized_string)
    return sanitized_string[:40]

def lambda_handler(event, context):
    username_partition=glue.get_partitions(
        DatabaseName=database_name,
        TableName=table_name
    )
    
    partition_values_list=[p["Values"] for p in username_partition["Partitions"]]
    partition_values=[]
    
    for partition in partition_values_list:
        for value in partition:
            partition_values.append(value)
    
    partition_values_sanitized=[sanitize(value) for value in partition_values]
    result=zip(partition_values, partition_values_sanitized)
    result=list(map(lambda args: {"partition": args[0], "partition_sanitized": args[1]}, result))
    
    return result