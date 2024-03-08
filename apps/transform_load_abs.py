import json

from abc import ABC, abstractmethod 
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import max
from delta import *
from models.job import Job
from etl_config import *
from common_utils.database import with_transaction

def _load_spark_config(env, app_name):
    """
        Load Spark session
    """
    builder = SparkSession.builder.appName(app_name) \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.cores.max", "1") \
        .config("spark.shuffle.service.enabled", "false") \
        .config("spark.dynamicAllocation.enabled", "false")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def _get_data_from_sink(spark_config, sink_path):
    """
        Get data from sink database
    """
    return spark_config.read \
        .format("delta") \
        .load(sink_path)

def read_table_from_staging(spark_config, bucket_name, schema, db_name, table_name):
    start_time = datetime.utcnow()
    sink_path = f"file://{DATA_LAKE}/{bucket_name}/{db_name}/{schema}/{table_name}/{start_time.year}/{start_time.month}/{start_time.day}"
    df = _get_data_from_sink(spark_config, sink_path)
    return df

@with_transaction
def _save_log_to_etl_metadata(session, **kwargs):
    """
        Save job metadata extractor job to log's database
    """

    job = Job()

    job.db_name = kwargs['db_name']
    job.table_name = kwargs['table_name']
    job.action = kwargs['action']
    job.last_row_number = kwargs['last_row_number']
    job.status = kwargs['status']
    job.details = kwargs['details']
    job.job_date = kwargs['job_date']
    job.job_start = kwargs['job_start']
    job.job_end = kwargs['job_end']
    job.zone = kwargs['zone']

    session.add(job)
    session.flush()

class TransformLoad(ABC):
    def __init__(self, 
                env,
                bucket_name,
                zone,
                action,
                app_name
                ) -> None:
        self.spark = _load_spark_config(env, f"{app_name}")
        self.bucket_name = bucket_name
        self.zone = zone
        self.action = action

    @abstractmethod
    def transform(self, **kwargs):
        pass

    def load(self, **kwargs):
        global end_time

        start_time = datetime.utcnow()

        log4jLogger = self.spark._jvm.org.apache.log4j
        logger = log4jLogger.LogManager.getLogger(__name__)
        logger.info("pyspark script logger initialized")


        job_logging = {
                        'db_name': 'hieuut',
                        'action': self.action,
                        'zone': self.zone,
                        'job_date': f'{start_time.year}-{start_time.month}-{start_time.day}',
                    }

        try:
            new_data, table_name = self.transform(**kwargs)
            incoming_last_row_number = new_data.count()

            load_sink_path = f"file://{DATA_LAKE}/{self.bucket_name}/{table_name}/{start_time.year}/{start_time.month}/{start_time.day}"

            job_details = {
                        'sink_path': load_sink_path
                    }

            new_data.write.format('delta').mode('overwrite').save(load_sink_path)
            logger.info(f"Uploaded data of {table_name} to warehouse zone")

            end_time = datetime.utcnow()
            job_logging.update({
                            'status': SUCCESS_STATUS,
                            'job_start': start_time,
                            'job_end': end_time,
                            'last_row_number': incoming_last_row_number,
                            'details': json.dumps(job_details),
                            'table_name': table_name,
                        })

            _save_log_to_etl_metadata(**job_logging)
        except Exception as e:
            end_time = datetime.utcnow()
            job_details = {
                        'error_message': str(e)
                    }

            end_time = datetime.utcnow()
            job_logging.update({
                'status': ERROR_STATUS,
                'job_start': start_time,
                'job_end': end_time,
                'details': json.dumps(job_details),
                'table_name': '',
                'last_row_number': 0,
            })

            _save_log_to_etl_metadata(**job_logging)