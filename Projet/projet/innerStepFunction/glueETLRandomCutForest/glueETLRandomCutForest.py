import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv,
                          ['username',
                           'bucket_name'])

username=args['username']
bucket_name=args['bucket_name']

print("username is :", username)
print("Bucket name is :", bucket_name)


glueContext = GlueContext(SparkContext.getOrCreate())

df = glueContext.create_dynamic_frame_from_options(connection_type="s3", format="json", 
                                                   connection_options={"paths": [f"s3://{bucket_name}/{username}/ingestion_transform_output/"],
                                                                      "recurse": True}
                                                  )

df=df.toDF()
column_embeddings=df.select(col("embeddings"))
projection=column_embeddings
for i in range(0,10):
    projection=projection.withColumn(f"e{i}", projection["embeddings"].getItem(i))

projection=projection.drop(col("embeddings"))
df_projection=DynamicFrame.fromDF(projection, glue_ctx=glueContext, name="RCFTraining")
glueContext.write_dynamic_frame.from_options(frame=df_projection, connection_type="s3", 
                                             connection_options={"path":f"s3://{bucket_name}/{username}/training_RCF"},
                                             format="csv",
                                             format_options={
                                                "writeHeader": False
                                            })