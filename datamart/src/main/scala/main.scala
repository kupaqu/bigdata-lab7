import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.sql.functions._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer

object DataMart {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                            .appName("DataMart")
                            .master("local[*]")
                            .getOrCreate()

    val filePath = "data/openfoodfacts.csv" // CSV file path
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath) //.cast(DoubleType)
    val assembled = assembleVector(df)
    val scaled = scaleAssembledDataset(assembled)
    writeData(scaled)

    // writeData(transformed)

    println(s"Preprocessing completed.")

    // Start HTTP server to handle requests for data
    startHttpServer(spark)
    println(s"HTTP server started.")
  }

  def assembleVector(df: DataFrame): DataFrame = {
    // val columnToRemove = "sodium_100g"
    // println(s"Removing column $columnToRemove from DataFrame")
    // val df = df.drop(columnToRemove)
    // TODO: возможно перенести нормальный препроцесс из ноутбука?

    println("Assembling vector from DataFrame columns")
    val inputCols = df.columns
    val outputCol = "features"

    val vectorAssembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol(outputCol)
    val assembledDf = vectorAssembler.transform(df)
    println("Assembled vector schema: " + assembledDf.schema)

    return assembledDf
  }

  def scaleAssembledDataset(df: DataFrame): DataFrame = {
    println("Scaling assembled dataset")
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled")
    val scalerModel = scaler.fit(df)
    val scaledDf = scalerModel.transform(df)
    println("Scaled dataset schema: " + scaledDf.schema)

    return scaledDf
  }

  def writeData(df: DataFrame): Unit = {
    // JDBC URL for Oracle database
    val url = s"jdbc:clickhouse://clickhouse:9000"

    // Write preprocessed data to Oracle database
    df.write.format("jdbc")
      // .options(
      //   Map(
      //     "url" -> url,
      //     "driver" -> "com.github.housepower.jdbc.ClickHouseDriver",
      //     "dbtable" -> "default.openfoodfacts_processed",
      //     "user" -> "default",
      //     "password" -> "",
      //     "createTableOptions" -> "engine=MergeTree() order by (id)",
      //   )
      // )
      .option("url", url)
      .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
      .option("dbtable", "default.openfoodfacts_processed")
      .option("user", "default")
      .option("password", "")
      .option("createTableOptions", "engine=MergeTree() order by (id)")
      
      // .mode(SaveMode.Overwrite)
      .mode("overwrite")
      .save()
  }

  def readData(spark: SparkSession): DataFrame = {
    // JDBC URL for Oracle database
    val url = s"jdbc:clickhouse://clickhouse:9000"

    // Read data from Oracle table "foodfacts"
    val oracleData = spark.read.format("jdbc")
      .options(
        Map(
          "url" -> url,
          "driver" -> "com.github.housepower.jdbc.ClickHouseDriver",
          "dbtable" -> "default.openfoodfacts",
          "user" -> "default",
          "password" -> "",
        )
      )
      .load().drop("id")
    oracleData
  }

  def startHttpServer(spark: SparkSession): Unit = {
    import akka.actor.ActorSystem
    import akka.http.scaladsl.Http
    import akka.http.scaladsl.server.Directives._
    import akka.stream.ActorMaterializer

    implicit val system = ActorSystem("DataServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val route = path("data") {
      get {
        // Read data from Oracle
        val oracleData = readData(spark)
        println("got data:")
        oracleData.printSchema()
        // Send the data as a response
        complete(oracleData.toJSON.collect().mkString("[", ",", "]"))
      }
    }

    val bindingFuture = Http().bindAndHandle(route, "0.0.0.0", 9000)
    println(s"Server online at http://localhost:9000/")
  }
}