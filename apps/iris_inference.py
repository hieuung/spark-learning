# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler, IndexToString

if __name__ == '__main__':
    # Create SparkSession 
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Iris infront") \
        .getOrCreate() 
    
    log4jLogger = spark._jvm.org.apache.log4j
    logging = log4jLogger.LogManager.getLogger(__name__)
    logging.info("pyspark script logger initialized")

    # Create dataframe from resources
    df = spark.read.option("header", True)\
                    .option("inferSchema", True)\
                    .csv('./apps/resources/Iris.csv')

    path = './apps/resources/'
    model_path = path + "/model/bestModel"

    pipeline = PipelineModel.read().load(model_path)

    decode = IndexToString(inputCol= 'prediction', outputCol= 'predict_label', labels= pipeline.stages[0].labels)

    input = df.sample(False, 0.1, seed=0).limit(5)

    output = pipeline.transform(input)

    output_with_label = decode.transform(output)

    output_with_label.show()