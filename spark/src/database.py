import configparser
import clickhouse_connect

from typing import Dict
from logger import Logger

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import monotonically_increasing_id 
from clickhouse_connect.driver.client import Client

SHOW_LOG = True

class Database:
    def __init__(self, spark: SparkSession):
        config = configparser.ConfigParser()
        config.read('config.ini')

        self.host = config['clickhouse']['host']
        self.http_port = config['clickhouse']['http_port']
        self.client_port = config['clickhouse']['client_port']
        self.driver = config['spark']['driver']

        self.spark = spark
        self.jdbcUrl = f'jdbc:clickhouse://{self.host}:{self.client_port}'

        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self.client = self._get_client(self.host, self.http_port)

    def _get_client(self, host: str, port: str) -> Client:
        client = clickhouse_connect.get_client(host=host,
                                               port=port,
                                               username='default',
                                               password='')
        self.log.info('Connected to ClickHouse.')
        
        return client
    
    def create_db(self, db_name: str):
        self.client.command(f'CREATE DATABASE IF NOT EXISTS {db_name}')
        self.log.info(f'Created {db_name} database.')

    def create_dbtable(self, dbtable: str, columns: Dict):
        cols = ''
        for k, v in columns.items():
            cols += f'`{k}` {v}, '
        self.client.command(f'''
            CREATE TABLE IF NOT EXISTS {dbtable} 
            (
                {cols}
            ) ENGINE = MergeTree()
            ORDER BY tuple();  -- No specific order needed
        ''')
        self.log.info(f'Created {dbtable} table.')

    def insert_df(self, dbtable: str, df: DataFrame):
        columns = ', '.join([f'`{col}`' for col in df.columns])
        placeholders = ', '.join(['%s' for _ in range(len(df.columns))])
        query = f'INSERT INTO {dbtable} ({columns}) VALUES ({placeholders})'
        try:
            data = [tuple(row) for row in df.to_numpy()]
            for row in data:
                self.client.command(query, row)
            self.log.info('Data inserted successfully.')
        except Exception as e:
            self.log.error(f'Error inserting data: {e}')

    def read_dbtable(self, dbtable: str) -> DataFrame:
        jdbcDF = self.spark.read \
            .format('jdbc') \
                .option('driver', self.driver) \
                    .option('url', self.jdbcUrl) \
                        .option('dbtable', dbtable) \
                            .option('user', 'default') \
                                .option('password', '') \
                                    .load() \
                                        .drop('id')

        return jdbcDF
    
    def write_dbtable(self, df: DataFrame, dbtable: str):
        df.select('*').withColumn('id', monotonically_increasing_id()) \
            .write \
                .mode('overwrite') \
                    .format('jdbc') \
                        .option('driver', self.driver) \
                            .option('url', self.jdbcUrl) \
                                .option('dbtable', dbtable) \
                                    .option('user', 'default') \
                                        .option('password', '') \
                                            .option('createTableOptions', 'engine=MergeTree() order by (id)') \
                                                .save()