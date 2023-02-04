import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.{SparkContext, sql}

import java.time.LocalDate


object users_items {

  val spark: SparkSession = SparkSession.builder()
    .appName("andrey.berezin.lab05")
    .getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  val sc: SparkContext = spark.sparkContext

  val inputDir: String = spark.sparkContext.getConf.get("spark.users_items.input_dir")
  val outputDir: String = spark.sparkContext.getConf.get("spark.users_items.output_dir")
  val update: String = spark.sparkContext.getConf.get("spark.users_items.update")

  def main(args: Array[String]): Unit = {
    val visits: sql.DataFrame = getPreparedVisitsData
    val purchases: sql.DataFrame = getPreparedPurchasesData.as("purchases")

    val visitsCols: Set[String] = visits.columns.toSet
    val purchasesCols: Set[String] = purchases.columns.toSet
    val bothDfCols: Set[String] = visitsCols ++ purchasesCols

    val unitedDf: sql.DataFrame = visits
      .select(addMissingCols(visitsCols, bothDfCols):_*)
      .union(
        purchases.select(addMissingCols(purchasesCols, bothDfCols):_*)
      )

    unitedDf.createOrReplaceTempView("res")

    val maxDate = spark.sql("select max(date) from res")
      .first()(0).toString.replace("-", "")

    val result: sql.DataFrame = unitedDf
      .drop("date")
      .groupBy("uid", "item_id")
      .count()
      .groupBy("uid")
      .pivot("item_id")
      .sum("count")
      .na.fill(0)

    if (update == "0") {
      result
        .write
        .format("parquet")
        .save(s"$outputDir/$maxDate")
    }

    else {
      val previousData: sql.DataFrame = getPreviousData

      val prevCols: Set[String] = previousData.columns.toSet
      val currentCols: Set[String] = result.columns.toSet
      val allCols: Set[String] = prevCols ++ currentCols

      val updatedResult: sql.DataFrame = previousData
        .select(addMissingCols(prevCols, allCols): _*)
        .union(
          result.select(addMissingCols(currentCols, allCols): _*)
        )

      updatedResult
        .groupBy("uid")
        .sum()
        .write
        .format("parquet")
        .save(s"$outputDir/$maxDate")
    }
  }

  private def normalizeItemName: UserDefinedFunction = udf(
    (category: String, prefix: String) =>
      prefix + category.toLowerCase().replace(" ", "_").replace("-", "_")
  )

  private def getPreparedVisitsData: sql.DataFrame = {
    val visits: sql.DataFrame = spark
      .read
      .format("json")
      .load(s"$inputDir/view")

    visits
      .withColumn("item_id", normalizeItemName(col("item_id"), lit("view_")))
  }

  private def getPreparedPurchasesData: sql.DataFrame = {
    val purchases: sql.DataFrame = spark
      .read
      .format("json")
      .load(s"$inputDir/buy")

    purchases
      .withColumn("item_id", normalizeItemName(col("item_id"), lit("buy_")))
  }

  private def addMissingCols(existing: Set[String], allNeeded: Set[String]): List[Column] = {
    allNeeded.toList.map {
      case x if existing.contains(x) => col(x)
      case x => lit(null).as(x)
    }
  }

  private def getPreviousData: sql.DataFrame = {
    val lastDirName = getLastDirName
    spark
      .read
      .format("parquet")
      .load(s"$outputDir/$lastDirName")
  }

  private def getLastDirName: String = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path(s"$outputDir"))
      .filter(_.isDirectory)
      .map(_.getPath.getName)
      .sortBy(x => LocalDate.parse(x).toEpochDay)
      .slice(-1, 1)(0)
  }
}