import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from math import floor
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv,
                          ['username',
                           'bucket_name',
                           'database_name',
                           'table_name'])

username=args['username']
bucket_name=args['bucket_name']
database_name=args['database_name']
table_name=args['table_name']

print("Database name is :", database_name)
print("username is :", username)
print("Bucket name is :", bucket_name)
print("Table name is :", table_name)

glueContext = GlueContext(SparkContext.getOrCreate())

# Python3 implementation of above approach
# Function to calculate the
# Jaro Similarity of two strings
def jaro_distance(s1, s2) :

    # If the strings are equal
    if (s1 == s2) :
        return 1.0;

    # Length of two strings
    len1 = len(s1);
    len2 = len(s2);

    if (len1 == 0 or len2 == 0) :
        return 0.0;

    # Maximum distance upto which matching
    # is allowed
    max_dist = (max(len(s1), len(s2)) // 2 ) - 1;

    # Count of matches
    match = 0;

    # Hash for matches
    hash_s1 = [0] * len(s1) ;
    hash_s2 = [0] * len(s2) ;

    # Traverse through the first string
    for i in range(len1) :

        # Check if there is any matches
        for j in range( max(0, i - max_dist),
                    min(len2, i + max_dist + 1)) :
            
            # If there is a match
            if (s1[i] == s2[j] and hash_s2[j] == 0) :
                hash_s1[i] = 1;
                hash_s2[j] = 1;
                match += 1;
                break;
        
    # If there is no match
    if (match == 0) :
        return 0.0;

    # Number of transpositions
    t = 0;

    point = 0;

    # Count number of occurrences
    # where two characters match but
    # there is a third matched character
    # in between the indices
    for i in range(len1) :
        if (hash_s1[i]) :

            # Find the next matched character
            # in second string
            while (hash_s2[point] == 0) :
                point += 1;

            if (s1[i] != s2[point]) :
                point += 1;
                t += 1;
            else :
                point += 1;
                
        t /= 2;

    # Return the Jaro Similarity
    return ((match / len1 + match / len2 +
            (match - t) / match ) / 3.0);

# Jaro Winkler Similarity
def jaro_Winkler(s1, s2) :

    jaro_dist = jaro_distance(s1, s2);

    # If the jaro Similarity is above a threshold
    if (jaro_dist > 0.7) :

        # Find the length of common prefix
        prefix = 0;

        for i in range(min(len(s1), len(s2))) :
        
            # If the characters match
            if (s1[i] == s2[i]) :
                prefix += 1;

            # Else break
            else :
                break;

        # Maximum of 4 characters are allowed in prefix
        prefix = min(4, prefix);

        # Calculate jaro winkler Similarity
        jaro_dist += 0.1 * prefix * (1 - jaro_dist);

    return jaro_dist;

# This code is contributed by AnkitRai01

df = glueContext.create_dynamic_frame_from_catalog(
    database=database_name,
    table_name=table_name,
    push_down_predicate=f"username='{username}'"
    )

df_batch_transform = df.toDF().select("encodeur")

def hash_array_udf(array):
    def hasheur(input):
        return hash(input) & 0x1fffff
    string = ''.join([str(number) for number in array])
    hashage = hasheur(string)
    return hashage

hash_array = udf(hash_array_udf, IntegerType())
hashed_df = df.toDF().withColumn('hash', hash_array(col("encodeur")))
a, b=hashed_df.alias("a"), hashed_df.alias("b")
crossed=a.join(b, col("a.hash")< col("b.hash"))
columns = ["verb","username","groups","useragent","sourceips","resource","subresource","name","namespace","impersonateduser"]

w = [3,4,4,2,1,2,2,2,2,4]

def similarity_udf(
    verbA, verbB,
    usernameA, usernameB,
    groupsA, groupsB,
    useragentA, useragentB,
    sourceipsA, sourceipsB,
    resourceA, resourceB,
    subresourceA, subresourceB,
    nameA, nameB,
    namespaceA, namespaceB,
    impersonateduserA, impersonateduserB
):
    simil = 0
    simil += w[0] * jaro_Winkler(verbA, verbB)
    simil += w[1] * jaro_Winkler(usernameA, usernameB)
    simil += w[2] * jaro_Winkler(groupsA, groupsB)
    simil += w[3] * jaro_Winkler(useragentA, useragentB)
    simil += w[4] * jaro_Winkler(sourceipsA, sourceipsB)
    simil += w[5] * jaro_Winkler(resourceA, resourceB)
    simil += w[6] * jaro_Winkler(subresourceA, subresourceB)
    simil += w[7] * jaro_Winkler(nameA, nameB)
    simil += w[8] * jaro_Winkler(namespaceA, namespaceB)
    simil += w[9] * jaro_Winkler(impersonateduserA, impersonateduserB)
    simil /= sum(w)
    
    return simil

similarity = udf(similarity_udf, FloatType())
final = crossed.withColumn("similarity", similarity("a.verb","b.verb",
                                                    "a.username","b.username",
                                                    "a.groups","b.groups",
                                                    "a.useragent","b.useragent",
                                                    "a.sourceips","b.sourceips",
                                                    "a.resource","b.resource",
                                                    "a.subresource","b.subresource",
                                                    "a.name","b.name",
                                                    "a.namespace","b.namespace",
                                                    "a.impersonateduser","b.impersonateduser"))

df_training = final.select("a.encodeur","b.encodeur","similarity")
df_batch_transform = df_batch_transform.selectExpr("encodeur as in0")
df_training = df_training.selectExpr("a.encodeur as in0","b.encodeur as in1","similarity as label")
df_training=DynamicFrame.fromDF(df_training, glue_ctx=glueContext, name="Object2VecTraining")
df_batch_transform = DynamicFrame.fromDF(df_batch_transform, glue_ctx=glueContext, name="Object2VecInference")
glueContext.write_from_options(df_training,"s3", connection_options={"path":f"s3://{bucket_name}/{username}/training_O2V"}, format="json")
glueContext.write_from_options(df_batch_transform,"s3", connection_options={"path":f"s3://{bucket_name}/{username}/ingestion_transform"}, format="json")