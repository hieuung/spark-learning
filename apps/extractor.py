import pathlib

ETL_HOME_PATH = pathlib.Path(__file__).parent.resolve()

import os
import json

from datetime import datetime
from common_utils.yaml import yaml_load
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, monotonically_increasing_id, col, max as _max
from delta import *
from models.job import Job
from sqlalchemy import func
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

def _get_database_connection(env):
    """
        Get database connection configuration
    """
    db_config = yaml_load(os.path.join(ETL_HOME_PATH, "database.yaml"))
    return db_config[env]

def _load_tables_config(db_name, table_name=None, mode="full_load"):
    """
        Get source's database configuration file .yaml with mode 
    """
    assert db_name in [
        'hieuut'
    ]

    table_config = yaml_load(os.path.join(f"{ETL_HOME_PATH}/table_config", f"{db_name}.yaml"))

    if table_name:
        return table_config[mode][table_name]

    return table_config[mode]

def _get_dataframe_by_table_name(env, spark_config, db_connection, db_name, schema, table_name):
    """
        Get data from source table
    """
    return spark_config.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_connection['host']}:{db_connection['port']}/{db_name}") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", f"{db_connection['username']}") \
        .option("password", f"{db_connection['password']}") \
        .option("dbtable", f"{schema}.{table_name}") \
        .load()

def _get_dataframe_by_query_statement(env, spark_config, db_connection, db_name, sql_statement):
    """
        Get data from source table using query
    """
    return spark_config.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_connection['host']}:{db_connection['port']}/{db_name}") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", f"{db_connection['username']}") \
        .option("password", f"{db_connection['password']}") \
        .option("query", sql_statement) \
        .load()

def _execute_single_table(single_table: str, table_config: dict) -> dict:
    """
        Extract single table from source database
    """
    if single_table not in table_config.keys():
        raise Exception(f"{single_table} does not exist")

    return {
        single_table: table_config.get(single_table, None)
    }

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
    
@with_transaction
def _get_last_row_number_from_log(session, zone, db_name, table_name, status= SUCCESS_STATUS) -> int:
    """
        Get `last_row_number` (recent the number of rows of a table) from log's database
    """
    rs = session.query(func.max(Job.last_row_number)).filter(
        Job.db_name == db_name,
        Job.action == action,
        Job.status == status,
        Job.table_name == table_name,
        Job.zone == zone
    ).first()

    return rs[0] if rs[0] is not None else 0

@with_transaction
def _get_last_checkpoint(session, zone, db_name, table_name, action):
    """
        Get `sink_path` of recent extractor's job from log's database
    """
    last_job_details = session.query(Job.details).filter(
        Job.zone == zone,
        Job.db_name == db_name,
        Job.table_name == table_name,
        Job.action == action,
        Job.status == SUCCESS_STATUS
    ).order_by(Job.id.desc()).first()

    if len(last_job_details) == 0:
        return None
    
    return json.loads(last_job_details[0]).get('sink_path')

@with_transaction
def _get_last_row_number_from_log_job_date(session, zone, db_name, table_name, action, job_date, status= SUCCESS_STATUS) -> int:
    """
        Get `sink_path` of recent extractor's job from log's database
    """

    last_row = session.query(Job.last_row_number).filter(
        Job.zone == zone,
        Job.db_name == db_name,
        Job.table_name == table_name,
        Job.action == action,
        Job.status == status,
        Job.job_date == job_date
    ).first()
    return last_row[0] if last_row else 0

@with_transaction
def _is_data_available_from_etl_log(session, zone, db_name, table_name, job_date):
    rs = session.query(Job).filter(
        Job.zone == zone,
        Job.db_name == db_name,
        Job.table_name == table_name,
        Job.job_date == job_date
    ).count()

    return bool(rs)

def _add_datetime_columns(dataframe, make_created=False, make_updated=False, current_date=datetime.utcnow()):
    df = None
    if make_created:
        df = dataframe.withColumn("CREATED_AT", lit(current_date.strftime("%Y-%m-%d")))

    if make_updated:
        df = dataframe.withColumn("UPDATED_AT", lit(current_date.strftime("%Y-%m-%d")))

    return df if df else dataframe

def _get_data_from_sink(spark_config, sink_path):
    """
        Get data from sink database
    """
    return spark_config.read \
        .format("delta") \
        .load(sink_path)

@with_transaction
def _get_list_sink_data_path(session, zone, job_date):
    return session.query(Job.table_name, Job.details).filter(
        Job.zone == zone,
        Job.job_date == job_date,
    ).all()

def run_raw_extractor(env, db_name, schema, bucket_name, zone, action, pre_check_last_day_checkpoint=False, single_table=None):
    global end_time

    table_config = _load_tables_config(db_name=db_name)

    spark = _load_spark_config(env, f"{db_name}_Extractor")

    db_connection = _get_database_connection(env)

    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("pyspark script logger initialized")

    if single_table:
        table_config = _execute_single_table(single_table, table_config)

    for table_name, config in table_config.items():
        logger.info(f"Start pulling {db_name}.{table_name}")

        start_time = datetime.utcnow()
        sink_path = f"file://{DATA_LAKE}/{bucket_name}/{db_name}/{schema}/{table_name}/{start_time.year}/{start_time.month}/{start_time.day}"
        logger.info(f"Sink path {sink_path}")
        
        job_details = {
            'sink_path': sink_path
        }

        df = _get_dataframe_by_table_name(
            env=env,
            spark_config=spark,
            db_connection=db_connection,
            db_name=db_name,
            schema= schema,
            table_name=table_name
        )

        if len(df.head(1)) == 0:
            logger.info(f"Extractor ignored: {db_name}.{table_name}")
            continue

        if not set(config.get('required_columns', [])).issubset(df.columns):
            if not config.get('required_columns'):
                raise Exception("required_columns is mandatory field in table_config folder. Check table yaml config")

            raise Exception(f"Dataframe of {table_name} is missing {config.get('required_columns')}")

        df = df.withColumn("ROW_NUMBER", monotonically_increasing_id() + 1)

        logger.info(f"Getting last row number of {db_name}.{table_name}")
        df_last_row_number = _get_dataframe_by_query_statement(
            env=env,
            spark_config=spark,
            db_connection=db_connection,
            db_name= db_name,
            sql_statement=f"SELECT COUNT(*) FROM {schema}.{table_name}"
        )

        df_last_row_number = int(df_last_row_number.first()[0])

        logger.info(f"Last row number of {db_name}.{table_name} : {df_last_row_number}")

        current_row_number = _get_last_row_number_from_log(db_name=db_name, zone= zone, table_name=table_name, status= SUCCESS_STATUS)
        save_etl_log = False

        df = _add_datetime_columns(
                dataframe=df,
                make_created=config.get('make_created_at'),
                make_updated=config.get('make_updated_at')
            )
        logger.info(f"Adding created_at and updated_at for {db_name}.{table_name}")

        if current_row_number == 0:
            save_etl_log = True

        save_changes = False
        changes = None
        is_updated = False

        logger.info(f"Last row number of {df_last_row_number}, current row number {current_row_number}")
        if df_last_row_number > current_row_number > 0 or df_last_row_number == current_row_number:
            sink_path_pre_check = None
            if pre_check_last_day_checkpoint:
                sink_path_pre_check = _get_last_checkpoint(
                    db_name=db_name,
                    table_name=table_name,
                    action=action,
                    zone= zone
                )

            current_sink_path = sink_path_pre_check if sink_path_pre_check is not None else sink_path
            current_data = _get_data_from_sink(spark, current_sink_path)
            changes = df.subtract(current_data)

            if len(changes.head(1)) > 0:
                save_etl_log = True
                save_changes = True
                is_updated = True

        job_details.update(config)

        job_logging = {
            'db_name': db_name,
            'table_name': table_name,
            'action': action,
            'last_row_number': df_last_row_number,
            'zone': RAW_ZONE,
            'job_date': f'{start_time.year}-{start_time.month}-{start_time.day}',
        }

        logger.info(f"Job logging {job_logging}")

        if save_etl_log:
            try:
                end_time = datetime.utcnow()
                job_logging.update({
                    'details': json.dumps(job_details),
                    'status': SUCCESS_STATUS,
                    'job_start': start_time,
                    'job_end': end_time
                })

                if df_last_row_number != current_row_number or is_updated:
                    if save_changes and changes is not None:
                        logger.info("Job save changes")
                        changes.write.format('delta').mode('append').save(sink_path)
                    else:
                        logger.info("Job save all")
                        df.write.format('delta').mode('append').save(sink_path)

                    logger.info(f"Uploaded data of {db_name}.{table_name} to Sink")

                    _save_log_to_etl_metadata(**job_logging)
                    logger.info(f"Saved log of {db_name}.{table_name} into etl_metadata")

            except Exception as e:
                job_details.update({
                    'error_message': str(e)
                })

                job_logging.update({
                    'details': json.dumps(job_details),
                    'status': ERROR_STATUS,
                    'job_start': start_time,
                    'job_end': end_time
                })

                _save_log_to_etl_metadata(**job_logging)

        logger.info(f"Extract raw finished")
        
def run_staging_extractor(env, db_name, bucket_name, action, job_date, from_zone='raw',to_zone='staging', single_table=None):

    list_data_path_from_zone = _get_list_sink_data_path(zone= from_zone, job_date=job_date)
    spark = _load_spark_config(env, f"Read {env}- data from Sink")
    start_time = datetime.utcnow()

    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("pyspark script logger initialized")

    if single_table:
        list_data_path_from_zone = [item for item in list_data_path_from_zone if single_table == item[0]]

    logger.info(f"List table {list_data_path_from_zone}")

    for table_name, path in list_data_path_from_zone:
        logger.info(f"Start transfer {db_name}.{table_name}")

        parse_raw_path_json = json.loads(path)
        staging_sink_path = f"file://{DATA_LAKE}/{bucket_name}/{db_name}/{schema}/{table_name}/{start_time.year}/{start_time.month}/{start_time.day}"

        incoming_data = _get_data_from_sink(spark, parse_raw_path_json.get('sink_path'))
        incoming_last_row_number = incoming_data.agg(_max('ROW_NUMBER')).head()["max(ROW_NUMBER)"]
        logger.info(f"Incoming last row number {incoming_last_row_number}")

        job_logging = {
                'db_name': db_name,
                'action': action,
                'zone': STAGING_ZONE,
                'table_name': table_name,
                'job_date': f'{start_time.year}-{start_time.month}-{start_time.day}',
            }

        logger.info(f"Job logging {job_logging}")

        job_details = {
                'sink_path': staging_sink_path
        }

        if not _is_data_available_from_etl_log(db_name=db_name, zone= to_zone, table_name=table_name, job_date=job_date):
            logger.info(f"Data {table_name} not available from etl log")
            
            job_logging.update(
                {
                    'details': json.dumps(job_details)
                }
            )

            try:
                current_row_number = _get_last_row_number_from_log_job_date(
                    db_name=db_name,
                    table_name=table_name,
                    action=action,
                    status= SUCCESS_STATUS,
                    zone=STAGING_ZONE,
                    job_date=job_date
                )

                if incoming_last_row_number > current_row_number:
                    incoming_data.write.format('delta').mode('append').save(staging_sink_path)
                    logger.info(f"Uploaded data of {db_name}.{table_name} to staging zone")

                    end_time = datetime.utcnow()
                    job_logging.update({
                        'status': SUCCESS_STATUS,
                        'job_start': start_time,
                        'job_end': end_time,
                        'last_row_number': incoming_last_row_number,
                    })

                    _save_log_to_etl_metadata(**job_logging)
            except Exception as e:
                logger.info(f"Data {table_name} available from etl log")
                job_details.update({
                    'error_message': str(e)
                })

                end_time = datetime.utcnow()
                job_logging.update({
                    'status': ERROR_STATUS,
                    'job_start': start_time,
                    'job_end': end_time,
                    'job_details': job_details
                })

                _save_log_to_etl_metadata(**job_logging)
        else:
            try:
                current_staging_data = _get_data_from_sink(spark, staging_sink_path)
                changes = incoming_data.subtract(current_staging_data)

                if len(changes.head(1)) > 0:
                    changes.write.format('delta').mode('append').save(staging_sink_path)
                    logger.info(f"Uploaded data of {db_name}.{table_name} to staging zone")

                    end_time = datetime.utcnow()
                    job_logging.update({
                        'status': SUCCESS_STATUS,
                        'job_start': start_time,
                        'job_end': end_time,
                        'last_row_number': incoming_last_row_number,
                    })

                    _save_log_to_etl_metadata(**job_logging)
            except Exception as e:
                job_details.update({
                    'error_message': str(e)
                })

                job_logging.update({
                    'status': ERROR_STATUS,
                    'job_start': start_time,
                    'job_end': end_time,
                    'job_details': job_details
                })

                _save_log_to_etl_metadata(**job_logging)

if __name__ == '__main__':
    env= 'hieu_env'
    db_name = 'hieuut'
    schema = 'inventory'
    action = 'extract'

    os.environ['ENV_CONFIG'] = json.dumps(
        {
            'SQL_URI': 'postgresql://hieuut:hieuut@10.99.193.236:5432/hieuut'
        }
    )

    run_raw_extractor(env, 
                      db_name= db_name, 
                      schema= schema,
                      bucket_name='raw-data', 
                      zone= 'raw',
                      action= action, 
                      pre_check_last_day_checkpoint=True, 
                      single_table= None
                      )

    run_staging_extractor(env,
                          db_name,
                          bucket_name='staging-data',
                          from_zone='raw',
                          to_zone='staging',
                          action='extract',
                          job_date='2024-3-8',
                          single_table=None)