import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.{SparkContext, sql}


object features {

  val spark: SparkSession = SparkSession.builder()
    .appName("andrey.berezin.lab05")
    .getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  val sc: SparkContext = spark.sparkContext

  def main(args: Array[String]): Unit = {
    val logsJson: sql.DataFrame = readLog("json")
    val logsParquet: sql.DataFrame = readLog("parquet")

    val unionLog: sql.DataFrame = logsJson.union(logsParquet)
    unionLog.createOrReplaceTempView("unionLog")

    val top1000: sql.DataFrame = unionLog
      .groupBy("domain")
      .count()
      .filter("domain is not null")
      .orderBy(col("count").desc)
      .limit(1000)
      .orderBy("domain")

    top1000.createOrReplaceTempView("top1000")

    val filteredDf:sql.DataFrame = spark.sql(
      "SELECT * FROM unionLog WHERE domain IN (SELECT domain FROM top1000)"
    )

    val dfWithTsMarks:sql.DataFrame = filteredDf
      .withColumn("day", date_format(from_unixtime(col("visit.timestamp") / 1000), "E"))
      .withColumn("hour", hour(from_unixtime(col("visit.timestamp") / 1000)))
      .withColumn("fraction_work_hours", col("hour").between(9, 17))
      .withColumn("fraction_evening_hours", col("hour").between(18, 23))
      .drop("visit")

    dfWithTsMarks.createOrReplaceTempView("dfWithTsMarks")

    val pivotDays: sql.DataFrame = spark.sql(
      "SELECT " +
          "uid, " +
          "domain, " +
          "'web_day_' || lower(day) as day, " +
          "'web_hour_' || CAST(hour AS String) as hour, " +
          "CAST(fraction_work_hours AS Int) as fraction_work_hours, " +
          "CAST(fraction_evening_hours AS Int) as fraction_evening_hours " +
        "FROM dfWithTsMarks"
    ).groupBy("uid", "domain", "fraction_work_hours", "fraction_evening_hours", "hour")
      .pivot("day")
      .count()
      .na.fill(0)

    val pivotDf: sql.DataFrame = pivotDays
      .groupBy("uid", pivotDays.columns.filter(x => x != "uid" && x != "hour"): _*)
      .pivot("hour")
      .count()
      .na.fill(0)

    pivotDf.createOrReplaceTempView("pivotDf")

    val aggCols: String = pivotDf
      .columns
      .filter(x => x != "uid" && x != "domain")
      .map(x => s"sum($x) as $x")
      .mkString(", ")

    val aggDf: sql.DataFrame = spark.sql(
      "SELECT " +
        "uid, " +
        "collect_list(domain) as domains, " +
        aggCols +
      " FROM pivotDf GROUP BY uid"
    )

    val topArr: Array[String] = top1000.select("domain").collectAsList().toArray.map(x => x.toString)

    val cvmDf: sql.DataFrame = new CountVectorizerModel(topArr)
      .setInputCol("domains")
      .setOutputCol("domains_features")
      .transform(aggDf)

    val previousMatrix: sql.DataFrame = readPreviousMatrix()

    val currentCols: Set[String] = cvmDf.columns.toSet
    val previousCols: Set[String] = previousMatrix.columns.toSet
    val bothDfCols: Set[String] = currentCols ++ previousCols

    val unitedDf: sql.DataFrame = cvmDf
      .select(addMissingCols(currentCols, bothDfCols): _*)
      .unionByName(
        previousMatrix.select(addMissingCols(previousCols, bothDfCols): _*)
      )

    unitedDf
      .write
      .format("parquet")
      .save("/user/andrey.berezin/features")
  }

  private def readLog(logFormat: String): sql.DataFrame = {
    spark
      .read
      .format(logFormat)
      .load(s"/labs/laba03/weblogs.$logFormat")
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

  private def addMissingCols(existing: Set[String], allNeeded: Set[String]): List[Column] = {
    allNeeded.toList.map {
      case x if existing.contains(x) => col(x)
      case x => lit(null).as(x)
    }
  }
}
