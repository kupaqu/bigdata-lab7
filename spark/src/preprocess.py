from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import VectorAssembler, StandardScaler

from database import Database

def read_csv(path: str, spark: SparkSession) -> DataFrame:
    df = spark.read.csv(path, header=True, inferSchema=True)

    return df

def assemble(df: DataFrame) -> DataFrame:
    vecAssembler = VectorAssembler(inputCols=df.columns, outputCol='features')
    data = vecAssembler.transform(df)

    return data

def scale(df: DataFrame) -> DataFrame:
    standardScaler = StandardScaler(inputCol='features', outputCol='scaled')
    model = standardScaler.fit(df)
    data = model.transform(df)

    return data

def df_to_dbtable(df: DataFrame, dbtable: str, database: Database):
    database.write_dbtable(df, dbtable)

def dbtable_to_df(dbtable: str, database: Database) -> DataFrame:
    return database.read_dbtable(dbtable)