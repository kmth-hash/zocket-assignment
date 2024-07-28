from pyspark.sql import SparkSession 

# initialize spark session 
def sparkInit():
    spark = SparkSession.builder.config('spark.jars','/home/mcmac/prj/tools/spark/spark/jars/mysql-connector-j-9.0.0.jar').appName("Assignment-Q23").getOrCreate()
    print('Spark session initiated : ')
    return spark


def readFromDB(spark) : 
    print('Reading data from table : ')
    df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/zocket") \
    .option("dbtable", "testDB4") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

    df.show()

def main():
    spark = sparkInit()
    
    readFromDB(spark)
    spark.stop()

if __name__=="__main__":
    main()
