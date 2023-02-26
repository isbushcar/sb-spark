import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer, StringIndexerModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, sql}



object test {

  val spark: SparkSession = SparkSession.builder()
    .appName("andrey.berezin.lab07")
    .getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  val sc: SparkContext = spark.sparkContext

  val kafkaServer: String = "spark-master-1:6667"
  val modelPath: String = spark.sparkContext.getConf.get("spark.mlproject.model_path")
  val sourceTopic: String = spark.sparkContext.getConf.get("spark.mlproject.source_topic")
  val targetTopic: String = spark.sparkContext.getConf.get("spark.mlproject.target_topic")
  val checkpointsLocation: String = spark.sparkContext.getConf.get("spark.mlproject.checkpoints_dir")

  def main(args: Array[String]): Unit = {

    val schema: StructType = StructType(Seq(
      StructField("uid", StringType, nullable = false),
      StructField("visits",
        ArrayType(
          StructType(Seq(
            StructField("timestamp", LongType, nullable = true),
            StructField("url", StringType, nullable = true)
            )),
          containsNull = true),
        nullable = true)
    ))

    val source: sql.DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", sourceTopic)
      .option("startingOffsets", "earliest")
      .load()
      .select(trim(col("value"), "[] ").as("val"))
      .withColumn("val", from_json(col("val"), schema))
      .select("val.*")
      .select(col("uid"), explode(col("visits")).as("visit"))
      .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
      .withColumn("domains", regexp_replace(col("host"), "www.", ""))
      .drop("host", "visit")
      .groupBy("uid")
      .agg(collect_list("domains").as("domains"))

    val model: PipelineModel = PipelineModel.load(modelPath)
    val result: sql.DataFrame = model
      .transform(source)
      .select(col("uid"), col("gender_age_reversed").as("gender_age"))

    result
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("topic", targetTopic)
      .option("checkpointLocation", checkpointsLocation)
      .outputMode("complete")
      .start()
  }

}
