import configparser

from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from logger import Logger
from preprocess import read_csv, assemble, scale, df_to_dbtable, dbtable_to_df
from database import Database

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
    
    db = Database(spark)
    db.create_db('lab6')

    data_path = config['data']['openfoodfacts']

    # load csv to clickhouse
    df = read_csv(data_path, spark)
    df_to_dbtable(df, 'lab6.openfoodfacts', db)

    # download from clickhouse
    df = dbtable_to_df('lab6.openfoodfacts', db)
    # df = assemble(df)
    # df = scale(df)

    kmeans = KmeansPredictor()
    preds = kmeans.fit_predict(df)

    # load predictions to clickhouse
    df_to_dbtable(preds.select('prediction'), 'lab6.predictions', db)

    spark.stop()