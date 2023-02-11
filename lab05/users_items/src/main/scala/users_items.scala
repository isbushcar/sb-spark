import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.{SparkContext, sql}

import java.time.LocalDate
import java.time.format.DateTimeFormatter


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
    val purchases: sql.DataFrame = getPreparedPurchasesData

    val visitsCols: Set[String] = visits.columns.toSet
    val purchasesCols: Set[String] = purchases.columns.toSet
    val bothDfCols: Set[String] = visitsCols ++ purchasesCols

    val unitedDf: sql.DataFrame = visits
      .select(addMissingCols(visitsCols, bothDfCols):_*)
      .unionByName(
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
      val minDate = spark.sql("select min(date) from res")
        .first()(0).toString

      val previousDate = LocalDate
        .parse(minDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        .minusDays(1)
        .toString
        .replace("-", "")

      val previousData: sql.DataFrame = getPreviousData(previousDate)

      val prevCols: Set[String] = previousData.columns.toSet
      val currentCols: Set[String] = result.columns.toSet
      val allCols: Set[String] = prevCols ++ currentCols

      val updatedResult: sql.DataFrame =
        previousData.select(addMissingCols(prevCols, allCols): _*)
        .unionByName(
          result.select(addMissingCols(currentCols, allCols): _*)
        )

      val sumSql: String = updatedResult
        .columns.filter(x => x != "uid")
        .map(x => s"coalesce(sum($x), 0) as $x")
        .mkString(", ")

      updatedResult.createOrReplaceTempView("res")

      val finalResult: sql.DataFrame = spark.sql(
        s"select uid, $sumSql from res group by uid"
      )

      finalResult
        .write
        .format("parquet")
        .save(s"$outputDir/$maxDate")
    }
  }

  private def columnsToNormalizeItemId(columns: Array[String], prefix: String): String = {
    s"${columns.mkString(", ")}".replace(
      "item_id",
      s"'$prefix' || replace(replace(lower(item_id), ' ', '_'), '-', '_') as item_id")
  }

  private def getPreparedVisitsData: sql.DataFrame = {
    val visits: sql.DataFrame = spark
      .read
      .format("json")
      .load(s"$inputDir/view")

    visits.createOrReplaceTempView("visits")
    spark.sql(s"SELECT " + columnsToNormalizeItemId(visits.columns, "view_") + " FROM visits")
  }

  private def getPreparedPurchasesData: sql.DataFrame = {
    val purchases: sql.DataFrame = spark
      .read
      .format("json")
      .load(s"$inputDir/buy")

    purchases.createOrReplaceTempView("purchases")
    spark.sql(s"SELECT " + columnsToNormalizeItemId(purchases.columns, "buy_") + " FROM purchases")
  }

  private def addMissingCols(existing: Set[String], allNeeded: Set[String]): List[Column] = {
    allNeeded.toList.map {
      case x if existing.contains(x) => col(x)
      case x => lit(null).as(x)
    }
  }

  private def getPreviousData(dirName: String): sql.DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"$outputDir/$dirName")
  }
}
