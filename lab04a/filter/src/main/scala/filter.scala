import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, sql}


object filter {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("andrey.berezin.lab04a")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val sc: SparkContext = spark.sparkContext

    val kafkaServer: String = "spark-master-1:6667"
    val inputTopic: String = spark.sparkContext.getConf.get("spark.filter.topic_name")
    val offset: String = spark.sparkContext.getConf.get("spark.filter.offset")
    val outputDirPrefix: String = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")

    val inputDf: sql.DataFrame = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", inputTopic)
      .option("startingOffsets",
        if (offset.contains("earliest"))
          offset
        else {
          "{\"" + inputTopic + "\":{\"0\":" + offset + "}}"
        }
      )
      .load()

    inputDf.createOrReplaceTempView("inputDf")
    val values: sql.DataFrame = spark.sql("select trim(BOTH '[] ' FROM value) as val from inputDf")

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

    val addedDate: sql.DataFrame = parsedDf
      .withColumn("date", to_date(from_unixtime(col("timestamp") / 1000)))
      .withColumn("p_date", date_format(col("date"), "yyyyMMdd"))

    val views: sql.DataFrame = addedDate.where("event_type = 'view'")
    views
      .write
      .format("json")
      .partitionBy("p_date")
      .save(s"$outputDirPrefix/view")

    val purchases: sql.DataFrame = addedDate.where("event_type = 'buy'")
    purchases
      .write
      .format("json")
      .partitionBy("p_date")
      .save(s"$outputDirPrefix/buy")

  }
}
