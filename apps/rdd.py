# Import SparkSession
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Create SparkSession 
      spark = SparkSession.builder \
          .master("local[1]") \
          .appName("RDD demo") \
          .getOrCreate() 

      log4jLogger = spark._jvm.org.apache.log4j
      logging = log4jLogger.LogManager.getLogger(__name__)
      logging.info("pyspark script logger initialized")

      logging.info('Hello world')

      # Create RDD from parallelize    
      dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
      rdd= spark.sparkContext.parallelize(dataList)

      logging.info(f'Number of elements in RDD {rdd.count()}')

      try:
            # Write RDD to source
            rdd.saveAsTextFile("./apps/resources/rdd.txt")
      except Exception as e:
            logging.warn(e)

      spark.stop()