import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, sql}


object agg {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("andrey.berezin.lab04b")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val sc: SparkContext = spark.sparkContext

    val kafkaServer: String = "spark-master-1:6667"
    val inputTopic: String = "andrey_berezin"
    val outputTopic: String = "andrey_berezin_lab04b_out"
    val checkpointsLocation: String = "/user/andrey.berezin/04b_checkpoints"

    val values: sql.DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", inputTopic)
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING) as val")

    val schema: StructType = StructType(Seq(
      StructField("event_type", StringType),
      StructField("category", StringType),
      StructField("item_id", StringType),
      StructField("item_price", IntegerType),
      StructField("uid", StringType),
      StructField("timestamp", LongType)
    ))

    val parsedDf: sql.DataFrame = values
      .withColumn("val", from_json(col("val"), schema))
      .select("val.*")
      .withColumn("timestamp", date_trunc("hour", from_unixtime(col("timestamp") / 1000)))

    def createSink(df: sql.DataFrame)(batchFunc: (sql.DataFrame, Long) => Unit) = {
      df
        .writeStream
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .option("checkpointLocation", checkpointsLocation)
        .foreachBatch(batchFunc)
        .start()
        .awaitTermination()
    }

    createSink(parsedDf) { (df: sql.DataFrame, id) =>
      df.createOrReplaceTempView("filtered")

      val preAgg: sql.DataFrame = df.sparkSession.sql(
        """
        SELECT
          timestamp,
          CASE WHEN event_type = 'buy' THEN item_price ELSE 0 END AS revenue,
          CASE WHEN event_type = 'buy' THEN 1 ELSE 0 END AS purchases,
          CASE WHEN uid != 'null' THEN 1 ELSE 0 END AS visitors
        FROM filtered
        """)
      preAgg.createOrReplaceTempView("preAgg")

      val result: sql.DataFrame = preAgg
        .groupBy(window(col("timestamp"), "1 hours"))
        .agg(
          sum("revenue").as("revenue"),
          sum("purchases").as("purchases"),
          sum("visitors").as("visitors")
        )
        .withColumn("aov", col("revenue") / col("purchases"))
        .select("window.*", "revenue", "visitors", "purchases", "aov")
        .withColumnRenamed("start", "start_ts")
        .withColumn("start_ts", col("start_ts").cast("long"))
        .withColumnRenamed("end", "end_ts")
        .withColumn("end_ts", col("end_ts").cast("long"))

      result
        .toJSON
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaServer)
        .option("topic", outputTopic)
        .save()
    }
  }
}
