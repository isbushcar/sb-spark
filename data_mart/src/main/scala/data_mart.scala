import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, sql}

import java.net.{URL, URLDecoder}
import scala.util.Try


object data_mart {

  val pgUser: String = "andrey_berezin"
  val pgPass: String = "H6m2C3Ts"
  val pgAddress: String = "10.0.0.31:5432"

  val spark: SparkSession = SparkSession.builder().appName("andrey.berezin.lab03").getOrCreate()
  val sc: SparkContext = spark.sparkContext

  def main(args: Array[String]): Unit = {
    val clients: sql.DataFrame = getClientsData()
    val shopVisits: sql.DataFrame = getShopVisitsLog()
    val siteVisits: sql.DataFrame = getSiteLogs()
    val siteCategories: sql.DataFrame = getSiteCategories()

    val joinedShopCategories: sql.DataFrame = clients.join(
      shopVisits,
      "uid"
    ).toDF("uid", "age", "gender", "category")

    val pivotShopCategories: sql.DataFrame = joinedShopCategories
      .groupBy("uid", "age_cat", "gender", "category")
      .count()
      .groupBy("uid", "age_cat", "gender")
      .pivot("category")
      .sum("count")

    val visitsWithCategories: sql.DataFrame = siteVisits.join(
      siteCategories,
      siteVisits("url") === siteCategories("domain")
    ).drop("url", "domain")

    val pivotSiteCategories: sql.DataFrame = visitsWithCategories
      .groupBy("uid")
      .count()
      .groupBy("uid")
      .pivot("category")
      .sum("count")

    val result = pivotShopCategories.join(
      pivotSiteCategories,
      "uid"
    )

    writeResult(result)

  }

  def getClientsData(): sql.DataFrame = {
    val clients = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()
    val clientsByCategories: sql.DataFrame = clients.withColumn("age_cat", convertAgeToCategory(col("age")))
    clientsByCategories
      .drop("age")

  }

  def getShopVisitsLog(): sql.DataFrame = {
    val visits = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.read.metadata" -> "true",
        "es.nodes.wan.only" -> "true",
        "es.port" -> "9200",
        "es.nodes" -> "10.0.0.31",
        "es.net.ssl" -> "false"))
      .load("visits")
    visits
      .where("event_type = 'view'")
      .drop("event_type", "item_id", "item_price", "timestamp", "_metadata")
      .withColumn("category", normalizeCategory(col("category")))
  }

  def getSiteLogs(): sql.DataFrame = {
    val logs = spark.read.json("hdfs:///labs/laba03/weblogs.json")
    logs.select(col("uid"), explode(col("visits")).as("visits"))
      .select(col("uid"), col("visits.*"))
      .drop("timestamp")
      .withColumn("url", decodeUrlAndGetDomain(col("url")))
      .where("url != '' and url != '-'")
  }

  def getSiteCategories(): sql.DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$pgAddress/labdata")
      .option("dbtable", "domain_cats")
      .option("user", pgUser)
      .option("password", pgPass)
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  def convertAgeToCategory: UserDefinedFunction = udf(
    (age: Int) =>
    if (age <= 24) {
      "18-24"
    } else if (age <= 34) {
      "25-34"
    } else if (age <= 44) {
      "35-44"
    } else if (age <= 54) {
      "45-54"
    }
    else ">=55"
  )

  def normalizeCategory: UserDefinedFunction = udf(
    (category: String) =>
      "shop_" + category.toLowerCase().replace(" ", "_").replace("-", "_")
  )

  def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
    val decodedUrl: String = Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost
    }.getOrElse("")
    decodedUrl
      .stripPrefix("http://")
      .stripPrefix("https://")
      .stripPrefix("www.")
      .replaceFirst("/.*", ""): String
  })

  def writeResult(df: sql.DataFrame): Unit = {
    df.write
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$pgAddress/andrey_berezin")
      .option("dbtable", "clients")
      .option("user", pgUser)
      .option("password", pgPass)
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true)
      .mode("overwrite")
      .save()
  }

}
