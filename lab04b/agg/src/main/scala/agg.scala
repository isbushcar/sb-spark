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

    parsedDf
      .withColumn("timestamp", date_trunc("hour", from_unixtime(col("timestamp") / 1000)))

    def createSink(df: sql.DataFrame)(batchFunc: (sql.DataFrame, Long) => Unit) = {
      df
        .writeStream
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .option("checkpointLocation", checkpointsLocation)
        .outputMode("update")
        .foreachBatch(batchFunc)
    }

    createSink(parsedDf) { (df, id) =>
      df.cache()
      df.createOrReplaceTempView("parsedDf")

      val filtered = spark.sql(
        """SELECT
          *
          FROM parsedDf
          WHERE timestamp > (
              SELECT min(timestamp)
              FROM parsedDf
          )""")
      filtered.createOrReplaceTempView("filtered")

      val revenue: sql.DataFrame = spark.sql(
        """
        SELECT
          timestamp,
          sum(item_price) AS revenue
        FROM filtered
        WHERE event_type = 'buy'
        GROUP BY timestamp
      """)
      revenue.createOrReplaceTempView("revenueDf")

      val visitors: sql.DataFrame = spark.sql(
        """
        SELECT
          timestamp,
          count(*) AS visitors
        FROM filtered
        WHERE uid IS NOT NULL
        GROUP BY timestamp
      """)
      visitors.createOrReplaceTempView("visitorsDf")

      val purchases: sql.DataFrame = spark.sql(
        """
        SELECT
          timestamp,
          count(*) AS purchases
        FROM filtered
        WHERE event_type = 'buy'
        GROUP BY timestamp
      """)
      purchases.createOrReplaceTempView("purchasesDf")

      val result: sql.DataFrame = spark.sql(
        """
          SELECT
            CAST(timestamp AS Long) AS start_ts,
            CAST(timestamp AS Long) + 60 * 60 * 60 AS end_ts,
            revenue,
            visitors,
            purchases,
            revenue / purchases AS aov
          FROM revenueDf
          LEFT JOIN visitorsDf
            USING (timestamp)
          LEFT JOIN purchasesDf
            USING (timestamp)
      """)

      result
        .toJSON
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaServer)
        .option("topic", outputTopic)
        .save()

      df.unpersist
    }
      .start()
      .awaitTermination()
  }
}
