from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder, TrainValidationSplitModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

if __name__ == '__main__':
    # Create SparkSession 
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Iris-Model-Selection") \
        .getOrCreate() 
    
    log4jLogger = spark._jvm.org.apache.log4j
    logging = log4jLogger.LogManager.getLogger(__name__)
    logging.info("pyspark script logger initialized")

    # Create dataframe from resources
    df = spark.read.option("header", True)\
                    .option("inferSchema", True)\
                    .csv('./apps/resources/Iris.csv')


    # Encode labels
    string_indexer = StringIndexer(inputCol="Species", outputCol="Label")

    # Features columns
    vector_assembler = VectorAssembler(inputCols= ['SepalLengthCm','SepalWidthCm','PetalLengthCm','PetalWidthCm'], outputCol= 'features')

    # Learn a LogisticRegression model. This uses the parameters stored in lr.
    lr = LogisticRegression(featuresCol='features', labelCol='Label', maxIter=10, regParam=0.01)

    # Pipeline
    pipeline = Pipeline(stages= [string_indexer, vector_assembler, lr])

    # Evaluator
    evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', 
                                                  labelCol='Label',
                                                  metricName='f1')

    # Params grid
    grid = ParamGridBuilder() \
            .addGrid(lr.maxIter, [20, 30]) \
            .addGrid(lr.regParam, [0.1, 0.01]) \
            .build()

    # Train validation split
    tvs = TrainValidationSplit(estimator= pipeline, 
                               trainRatio=0.7,
                               estimatorParamMaps=grid, 
                               evaluator=evaluator, 
                               parallelism=1, 
                               seed=42)
    
    tvsModel = tvs.fit(df)

    path = './apps/resources/'
    model_path = path + "/model"
    tvsModel.write().save(model_path)
    tvsModelRead = TrainValidationSplitModel.read().load(model_path)

    logging.info(f'F1 scores: {evaluator.evaluate(tvsModelRead.transform(df))}')
