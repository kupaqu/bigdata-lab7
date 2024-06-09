from pyspark.sql import SparkSession
appName="Connect To clickhouse - via JDBC"
spark = SparkSession.builder.master('local').config("spark.driver.extraClassPath","jars/clickhouse-native-jdbc-shaded-2.7.1.jar").appName(appName).getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("driver", "com.github.housepower.jdbc.ClickHouseDriver") \
    .option("url", "jdbc:clickhouse://localhost:32769") \
    .option("user", "default") \
    .option("password", "") \
    .option("dbtable", "lab_6.openfoodfacts") \
    .load().show()