import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, sql}


object features {

  val spark: SparkSession = SparkSession.builder()
    .appName("andrey.berezin.lab05")
    .getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  val sc: SparkContext = spark.sparkContext

  def main(args: Array[String]): Unit = {
    val logsJson: sql.DataFrame = readLog()
    logsJson.createOrReplaceTempView("logsJson")

    val top1000: Array[String] = logsJson
      .groupBy("domain")
      .count()
      .filter("domain is not null")
      .orderBy(col("count").desc)
      .limit(1000)
      .orderBy("domain")
      .select("domain")
      .collectAsList()
      .toArray
      .map(x => x.toString.stripPrefix("[").stripSuffix("]"))

    val preparedForCvm: sql.DataFrame = spark.sql(
      "SELECT uid, collect_list(domain) as domains FROM logsJson GROUP BY uid"
    )

    val cvm: sql.DataFrame = new CountVectorizerModel(top1000)
      .setInputCol("domains")
      .setOutputCol("domain_features")
      .transform(preparedForCvm)
      .withColumn("domain_features", extractArray(col("domain_features")))
    cvm.createOrReplaceTempView("cvm")

    val parsedDaysAndHours: sql.DataFrame = logsJson
      .withColumn("day", date_format(from_unixtime(col("visit.timestamp") / 1000), "E"))
      .withColumn("hour", hour(from_unixtime(col("visit.timestamp") / 1000)))
    parsedDaysAndHours.createOrReplaceTempView("parsedDaysAndHours")

    val dfWithTsMarks: sql.DataFrame = spark.sql(
      """
       SELECT
           log.uid,
           log.day,
           log.hour,
           work.cnt / count(log.hour) OVER (PARTITION BY log.uid) AS web_fraction_work_hours,
           evening.cnt / count(log.hour) OVER (PARTITION BY log.uid) AS web_fraction_evening_hours
       FROM parsedDaysAndHours log
       LEFT JOIN
       (SELECT uid, count(hour) AS cnt
           FROM parsedDaysAndHours
           WHERE hour between 9 and 17
           GROUP BY uid) AS work
       USING (uid)
       LEFT JOIN
       (SELECT uid, count(hour) AS cnt
           FROM parsedDaysAndHours
           WHERE hour between 18 and 23
           GROUP BY uid) AS evening
       USING (uid)
       """
    )

    dfWithTsMarks.createOrReplaceTempView("dfWithTsMarks")

    val preparedDf: sql.DataFrame = spark.sql(
      """
         SELECT
          uid,
          'web_day_' || lower(day) as day,
          'web_hour_' || CAST(hour AS String) as hour,
          web_fraction_work_hours,
          web_fraction_evening_hours
        FROM dfWithTsMarks
        """
    )

    val pivotDays: sql.DataFrame = preparedDf
      .drop("hour")
      .groupBy("uid", "web_fraction_work_hours", "web_fraction_evening_hours")
      .pivot("day")
      .count()
      .na.fill(0)
    pivotDays.createOrReplaceTempView("pivotDays")

    val pivotHours: sql.DataFrame = preparedDf
      .drop("day")
      .groupBy("uid", "web_fraction_work_hours", "web_fraction_evening_hours")
      .pivot("hour")
      .count()
      .na.fill(0)
    pivotHours.createOrReplaceTempView("pivotHours")

    val pivotDf = spark.sql("""
        SELECT
          *
         FROM pivotDays
         FULL OUTER JOIN pivotHours
         USING (uid, web_fraction_work_hours, web_fraction_evening_hours)
      """)

    pivotDf.createOrReplaceTempView("pivotDf")

    val previousMatrix: sql.DataFrame = readPreviousMatrix()
    previousMatrix.createOrReplaceTempView("prev")

    val unitedDf: sql.DataFrame = spark.sql("""
        SELECT
          *
        FROM pivotDf
        LEFT JOIN cvm
        USING (uid)
        FULL OUTER JOIN prev
        USING (uid)
        """)

    unitedDf
      .write
      .format("parquet")
      .save("/user/andrey.berezin/features")
  }

  private def readLog(): sql.DataFrame = {
    spark
      .read
      .format("json")
      .load(s"/labs/laba03/weblogs.json")
      .select(col("uid"), explode(col("visits")).as("visit"))
      .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .drop("host")
  }

  private def readPreviousMatrix(): sql.DataFrame = {
    spark
      .read
      .format("parquet")
      .load("/user/andrey.berezin/users-items/20200429")
  }

  private def extractArray: UserDefinedFunction = udf(
    (feature: Any) => {
      feature.asInstanceOf[SparseVector].toArray.map(x => x.toInt)
    }
  )
}
