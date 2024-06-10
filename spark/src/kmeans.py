import configparser
import requests

from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from logger import Logger

SHOW_LOG = True

class KmeansPredictor:
    def __init__(self):
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self._evaluator = ClusteringEvaluator(featuresCol='scaled')
        
        self.log.info('KmeansEvaluator initialized.')

    def fit_predict(self, df: DataFrame) -> DataFrame:
        kmeans = KMeans(featuresCol='scaled', k=2)
        model = kmeans.fit(df)
        preds = model.transform(df)

        self.log.info('Kmeans training finished.')

        return preds

if __name__ == '__main__':
    logger = Logger(SHOW_LOG)
    log = logger.get_logger(__name__)

    config = configparser.ConfigParser()
    config.read('config.ini')

    spark = SparkSession.builder \
        .appName(config['spark']['app_name']) \
            .master(config['spark']['deploy_mode']) \
                .config('spark.driver.cores', config['spark']['driver_cores']) \
                    .config('spark.executor.cores', config['spark']['executor_cores']) \
                        .config('spark.driver.memory', config['spark']['driver_memory']) \
                            .config('spark.executor.memory', config['spark']['executor_memory']) \
                                .config('spark.driver.extraClassPath', config['spark']['clickhouse_connector']) \
                                    .getOrCreate()
    
    host = config['datamart']['host']
    port = config['datamart']['port']
    url = f'http://{host}:{port}/get_food_data'

    response = requests.get(url)
    df = spark.createDataFrame(response.json())
    log.info(f"Got processed dataset with schema: {df.schema}")

    kmeans = KmeansPredictor()
    preds = kmeans.fit_predict(df)
    log.info(f"Kmeans fitted and predicted.")

    spark.stop()