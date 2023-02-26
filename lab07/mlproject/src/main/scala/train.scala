import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer, StringIndexerModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, sql}


object train {

  val spark: SparkSession = SparkSession.builder()
    .appName("andrey.berezin.lab07")
    .getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  val sc: SparkContext = spark.sparkContext

  val trainingDataset: String = spark.sparkContext.getConf.get("spark.mlproject.training_dataset")
  val modelPath: String = spark.sparkContext.getConf.get("spark.mlproject.model_path")


  def main(args: Array[String]): Unit = {

    val training: sql.DataFrame = spark.read
      .format("json")
      .load(trainingDataset)
      .select(col("uid"), col("gender_age"), explode(col("visits")).as("visit"))
      .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
      .withColumn("domains", regexp_replace(col("host"), "www.", ""))
      .drop("host", "visit")
      .groupBy("uid", "gender_age")
      .agg(collect_list("domains").as("domains"))

    val cv: CountVectorizer = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer: StringIndexerModel = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(training)

    val lr: LogisticRegression = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val matchNames: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("gender_age_reversed")
      .setLabels(indexer.labels)

    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, matchNames))

    val model: PipelineModel = pipeline.fit(training)

    model.write.overwrite().save(modelPath)
  }
}
