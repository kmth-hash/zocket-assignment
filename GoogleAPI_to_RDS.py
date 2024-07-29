
# Import all necessary methods from pyspark package  
from pyspark.sql import SparkSession , Row 
from pyspark.sql.functions import lit , explode , col
from pyspark.sql.types import StringType , ArrayType , MapType , StructType , IntegerType , FloatType , DateType
#  import pandas to create pandas dataframe if reading from json file
# import pandas as pd 
# import requests package to make API calls 
import requests 
# json package to convert json response into objects and vice versa based on requirement 
import json 

# initialize spark session 
def sparkInit():
    try : 
        spark = SparkSession.builder.config('spark.jars','/home/mcmac/prj/tools/spark/spark/jars/mysql-connector-j-9.0.0.jar').appName("Assignment-Q").getOrCreate()
        print('Spark session initiated : ')

        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        return spark    
    except Exception as ex : 
        print('Error in started Spark session : ')
        print('Error Log : ',ex , sep="\n---->\n")

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
    try : 
        response = []
        # uses the spark session passed as parameter 
        # reads entire json file as option of multiline is enabled 
        df = spark.read.option('multiline',True).json(fileLocation)
        print('File read complete : ')
        # df.printSchema()
        # df.show()
        return df 
    except Exception as ex : 
        print('Error in reading JSON file. Please check if the file exists')
        print('Error Log : ',ex , sep="\n---->\n")

# Method to add transformation logic 
# Converting all the nested columns into a single dimensional schema for the dataframe 
# creates a new column to show the source of data (Default : Google Ads )
def transformDataFromGoogleAds(df , schemalist ,source='GoogleAds',exclusionCols=['id','name'] , sourceSchema=dict()) : 
    try : 
        mainCols = df.columns
        print('Applying Tranformations to Data')
        # New column creation 
        df = df.withColumn('source',lit(source))
        # iterate through each column header and create a new column with new name with separator of '_'
        for schemaIter in schemalist : 
            tempFieldName = schemaIter.replace('.','_') 
            tempType = sourceSchema.get(tempFieldName , 'str')
            reqFieldType = StringType()
            if tempType=='int' : 
                reqFieldType = IntegerType()
            elif tempType=='date' : 
                reqFieldType = DateType()
            elif tempType=='float' : 
                reqFieldType = FloatType()
            df = df.withColumn( tempFieldName, col(schemaIter).cast(reqFieldType))
            if schemaIter not in exclusionCols : 
                df = df.drop(schemaIter) # Drop the nested column   
        for i in mainCols : #initial nested parent column which is dropped 
            if i not in exclusionCols:
                df = df.drop(i)
        # df.printSchema()
        return df 
    except Exception as ex : 
        print('Error in applying schema changes to DF ')
        print('Error Log : ',ex , sep="\n---->\n")

def readSourceSchema():
    try : 
        jsonRes = dict()
        schema = StructType()
        with open('/home/mcmac/prj/zocket/sourceSchema.json' , 'r') as f : 
            x = (f.read())
            jsonRes = dict(json.loads(x))
        return jsonRes
    except Exception as ex : 
        print('Source Schema read fail : ')
        print('Error Log : ',ex , sep="\n---->\n")

def SourceSchemaGeneration():
    try : 
        jsonRes = readSourceSchema()
        schema = StructType()
        for eachField , eachFieldType in jsonRes.items() :
            if eachFieldType in ['int' , 'INT' , 'Integertype'] : 
                schema.add(eachField , IntegerType() , True)
            elif eachFieldType in ['str' , 'STR' , 'String'] : 
                schema.add(eachField , StringType() , True )
            elif eachFieldType in ['date' , 'DATE'] : 
                schema.add(eachField , DateType() , True) 
            elif eachFieldType in ['float' , 'FLOAT'] : 
                schema.add(eachField , FloatType() , True)
            else : 
                schema.add(eachField , StringType() , True )
        return schema 
    except Exception as ex : 
        print('Failed while creating structtype for source data : ')
        print('Error Log : ',ex , sep="\n---->\n")
        
        
# Multiple dataframes are combined (union specifically)
# All rows are stacked into one 
def finalData(dfList):
    try : 
        res = dfList[0]
        for df in dfList[1:] : 
            res = res.union(df)
        res = res.dropDuplicates() # Drops all duplicates 
        print("Final DataFrame created : ")
        return res 
    except Exception as ex : 
        print('Failed in final transformation : ')
        print('Error Log : ',ex , sep="\n---->\n")

# method to load data into Mysql database 
# Creates new table 
# Enable overwrite = true option to overwrite existing dbtable 
def loadDataIntoDB( df , dbTable ) : 
    try : 
        print(f"Loading data into : {dbTable}")
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
        .option("overwrite",True)\
        .option("password", "password") \
        .save()
        print('Data loaded into DB  ')
    except Exception as ex : 
        print('Error in loading data into DB ')
        print('Error Log : ',ex , sep="\n---->\n")
        

# method to return all the nested columns 
# it returns a list of all nested columns with delimiter of '.'
def flatten(schema, prefix=None):
    try : 
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
        return fields
    except Exception as ex : 
        pass 


def main():
    spark = None 
    try : 
        dbTable = 'testDB2'
        exclusionCols = ['id' , 'name']
        spark = sparkInit()
        srcData = mockGoogleAdsDataUsingSpark(spark,'/home/mcmac/prj/zocket/mockGoogleAdsResponse.json')
        schemaList = flatten(srcData.schema)
        srcData.show()

        responseSchema = readSourceSchema()
        enrichedData = transformDataFromGoogleAds(srcData , schemaList, 'GoogleAds' , exclusionCols,responseSchema).persist()
        # Let's assume we have another source of data , say Facebooks Ads API or DB 
        # We can store it in here 
        # In this example I have used the same Data source 
        enrichedDataFromAnotherSource = transformDataFromGoogleAds(srcData , schemaList, 'FacebookAds' , exclusionCols , responseSchema).persist()
        
        finalDF = finalData([enrichedData , enrichedDataFromAnotherSource]).persist()
        finalDF.printSchema()

        print('Previewing final data : ')
        finalDF.show()
        loadDataIntoDB(finalDF , dbTable)
        # enrichedData.show()
    except Exception as ex : 
        print('Process complete ! ')

        print('Error Log : ',ex , sep="\n---->\n")
    finally : 
        spark.stop()
    # Sparksession stopped 

if __name__=="__main__" : 
    try : 
        main()
    except Exception as ex : 
        print('Process exited : ')
        print('Error Log : ',ex , sep="\n---->\n")
    