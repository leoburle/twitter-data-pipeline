from pyspark.sql import SparkSession

def get_public_metrics(df):
    df.explode

if __name__ == '__main__':

    spark=SparkSession.builder.appName('TryingSpark').getOrCreate()

    spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs","false") 
    spark.conf.set("spark.executor.memory", "3g") 
    spark.conf.set("spark.driver.memory", "3g")
    # set spark logger level from config file
    spark.sparkContext.setLogLevel("WARN")

    data = spark.read.json('/Users/lburle/projects/others/twitter-data-pipeline/data', multiLine=True)

    data.printSchema()