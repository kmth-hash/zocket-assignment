
# Import all necessary methods from pyspark package  
from pyspark.sql import SparkSession , Row 
from pyspark.sql.functions import lit , explode , col
from pyspark.sql.types import StringType , ArrayType , MapType , StructType
#  import pandas to create pandas dataframe if reading from json file
import pandas as pd 
# import requests package to make API calls 
import requests 
# json package to convert json response into objects and vice versa based on requirement 
import json 

# initialize spark session 
def sparkInit():
    spark = SparkSession.builder.config('spark.jars','/home/mcmac/prj/tools/spark/spark/jars/mysql-connector-j-9.0.0.jar').appName("Assignment-Q").getOrCreate()
    print('Spark session initiated : ')

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    return spark

# Sample code to extract data from Google Ads API with required attributes 
def getDataFromGoogleAds(api_key , account_id) : 
    url = f'https://googleads.googleapis.com/v11/customers/{account_id}/googleAds:search'
    # pass api key for authentication 
    # returns json response 
    headers = {
        'Authorization' : f'Bearer {api_key}' ,
        'Content-Type' : 'application/json'
    }
    # Query to fetch required attributes 
    query = f'SELECT campaign.name, ad_group.name, ad.id, metrics.impression, metrics.clicks, metrics.groups FROM ad_group_ad where segments.date during LAST_30_DAYS ;'
    # Hit API endpoint (url) with headers and query 
    # returns response which can be passed to spark program for processing 
    response = requests.post(url, headers=headers , json=query)
    data = response.json()
    return pd.json_normalize(data['results'])

# Sample API response stored in json file for testing
# Reading the data from this file in this example 
def mockGoogleAdsDataUsingSpark(spark , fileLocation) : 
    response = []
    # uses the spark session passed as parameter 
    # reads entire json file as option of multiline is enabled 
    df = spark.read.option('multiline',True).json(fileLocation)
    # df.printSchema()
    # df.show()
    return df 

# Method to add transformation logic 
# Converting all the nested columns into a single dimensional schema for the dataframe 
# creates a new column to show the source of data (Default : Google Ads )
def transformDataFromGoogleAds(df , schemalist ,source='GoogleAds') : 
    mainCols = df.columns
    # New column creation 
    df = df.withColumn('Source',lit(source))

    # iterate through each column header and create a new column with new name with separator of '_'
    for schemaIter in schemalist : 
        df = df.withColumn(schemaIter.replace('.','_') , col(schemaIter))
        df = df.drop(schemaIter) # Drop the nested column 
    for i in mainCols : #initial nested parent column which is dropped 
        df = df.drop(i)
    # df.printSchema()
    return df 

# Multiple dataframes are combined (union specifically)
# All rows are stacked into one 
def finalData(dfList):
    res = dfList[0]
    for df in dfList[1:] : 
        res = res.union(df)
    res = res.dropDuplicates() # Drops all duplicates 
    return res 

# method to load data into Mysql database 
# Creates new table 
# Enable overwrite = true option to overwrite existing dbtable 
def loadDataIntoDB( df , dbTable ) : 
    print(dbTable)
    # Connects to localhost mySQL database using sql connector 
    # Host : localhost 
    # Database : zocket 
    # username : user
    # password : password 
    # table : passed through parameter dbTable
    df.write.format('jdbc')\
    .option('url' , 'jdbc:mysql://localhost:3306/zocket')\
    .option('driver','com.mysql.cj.jdbc.Driver')\
    .option("dbtable", dbTable) \
    .option("user", "username") \
    .option("password", "password") \
    .save()
    print('Data loaded into DB  ')

# method to return all the nested columns 
# it returns a list of all nested columns with delimiter of '.'
def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType

        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)
    print(*fields)
    return fields



dbTable = 'testDB1'
spark = sparkInit()
srcData = mockGoogleAdsDataUsingSpark(spark,'mockGoogleAdsResponse.json')
schemaList = flatten(srcData.schema)
# srcData.show()
# srcData.iteritems = srcData.items
enrichedData = transformDataFromGoogleAds(srcData , schemaList, 'GoogleAds' )
# Let's assume we have another source of data , say Facebooks Ads API or DB 
# We can store it in here 
# In this example I have used the same Data source 
enrichedDataFromAnotherSource = transformDataFromGoogleAds(srcData , schemaList, 'FacebookAds' )

finalDF = finalData([enrichedData , enrichedDataFromAnotherSource])
loadDataIntoDB(finalDF , dbTable)
# enrichedData.show()

spark.stop()
# Sparksession stopped 