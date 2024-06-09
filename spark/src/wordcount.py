import configparser

from pyspark.sql import SparkSession
from operator import add
from logger import Logger

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

SHOW_LOG = True

class WordCount:
    def __init__(self, spark: SparkSession):
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self.spark = spark
        self.log.info('Spark session created.')

        self.config = configparser.ConfigParser()
        self.config.read('config.ini')

        somewords_path = self.config['data']['somewords']
        self.somewords = self.spark.read.text(somewords_path).rdd.map(lambda r: r[0])
        self.log.info('somewords.txt loaded.')

    def count(self):
        word_counts = self.somewords.flatMap(lambda x: x.split(' ')) \
                            .map(lambda x: (x, 1)) \
                            .reduceByKey(add)
        
        self.log.info('Words counted.')

        return word_counts.collect()

if __name__ == '__main__':

    spark = SparkSession.builder.appName('WordCount').getOrCreate()
    wordcount = WordCount(spark)
    results = wordcount.count()

    for word, count in results:
        print(f'{word}: {count}')

    spark.stop()