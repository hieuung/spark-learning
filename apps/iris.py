# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler, IndexToString

if __name__ == '__main__':
    # Create SparkSession 
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Iris") \
        .getOrCreate() 
    
    log4jLogger = spark._jvm.org.apache.log4j
    logging = log4jLogger.LogManager.getLogger(__name__)
    logging.info("pyspark script logger initialized")

    # Create dataframe from resources
    df = spark.read.option("header", True)\
                    .option("inferSchema", True)\
                    .csv('./apps/resources/Iris.csv')

    # Create features dataframe from df
    df.printSchema()
    df.describe().show()

    logging.info("Split dataset")
    train, test = df.randomSplit([0.7, 0.3], seed=42)

    # Encode labels
    string_indexer = StringIndexer(inputCol="Species", outputCol="label")

    # Features columns
    vector_assembler = VectorAssembler(inputCols= ['SepalLengthCm','SepalWidthCm','PetalLengthCm','PetalWidthCm'], outputCol= 'features')

    # Learn a LogisticRegression model. This uses the parameters stored in lr.
    lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10, regParam=0.01)

    # Create pipeline
    pipeline = Pipeline(stages= [string_indexer, vector_assembler, lr])

    model = pipeline.fit(train)
    logging.info("Training finished")

    # Reverser encode
    reversed = IndexToString(inputCol='prediction', outputCol='output', labels=model.stages[0].labels)

    # Make predictions on test documents and print columns of interest.
    prediction = model.transform(test)
    selected = prediction.select("Id", "probability", "prediction", "Species")

    selected = reversed.transform(selected)
    selected.printSchema()

    for row in selected.collect():
        rid, _, _, real_label, output = row
        logging.info(
            f" Classification testing ({rid}) --> prediction= {output}, real_label ={real_label}"
        )