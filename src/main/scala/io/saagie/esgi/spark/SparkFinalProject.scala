package io.saagie.esgi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object SparkFinalProject {


  def main(args: Array[String]) {
    val PATH = "data/beers-breweries-and-beer-reviews"

    val spark = SparkSession.builder()
      .appName("Final-Project-Spark-G4")
      .getOrCreate()

    val schema_beer = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("brewery_id", StringType, true),
      StructField("state", StringType, true),
      StructField("country", StringType, true),
      StructField("style", StringType, true),
      StructField("availability", StringType, true),
      StructField("abv", FloatType, true),
      StructField("notes", StringType, true),
      StructField("retired", BooleanType, true)
    ))

    val beer = spark
      .read
      .option("inferSchema", "true")
      .option("header", "false")
      .schema(schema_beer)
      .csv(s"$PATH/beers.csv")
      .select("id", "name", "country", "style", "brewery_id")

    val schema_breweries = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("country", StringType, true),
      StructField("notes", StringType, true),
      StructField("types", StringType, true)
    ))

    val breweries = spark
      .read
      .option("inferSchema", "true")
      .option("header", "false")
      .schema(schema_breweries)
      .csv(s"$PATH/breweries.csv")
      .select("id", "types")

    val schema_reviews = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("username", StringType, true),
      StructField("date", DateType, true),
      StructField("text", StringType, true),
      StructField("look", IntegerType, true),
      StructField("smell", IntegerType, true),
      StructField("taste", IntegerType, true),
      StructField("feel", IntegerType, true),
      StructField("overall", IntegerType, true),
      StructField("score", IntegerType, true)
    ))

    val reviews = spark
      .read
      .option("inferSchema", "true")
      .option("header", "false")
      .schema(schema_reviews)
      .csv(s"$PATH/reviews.csv")
      .select("id", "score", "text")

    val df = beer.join(breweries, usingColumn = "id")
        .join(reviews, usingColumn = "id")
        .select("country", "name", "types", "score", "text", "brewery_id")
        .createOrReplaceTempView("tmp_data")

    val max_score = spark.sqlContext("SELECT MAX(score) FROM tmp_data")
    val max_avg = spark.sqlContext("SELECT MAX(AVG(score)) FROM tmp_data")
    val top_type = spark.sqlContext("SELECT brewery_id, types, count(brewery_id) as count_brasserie FROM tmp_data WHERE types like '%Beer-to-go%' GROUP BY brewery_id, types ORDER BY count(brewery_id) ASC LIMIT 10")

    val words = reviews.select("text")
      .filter("text is not null")
      .withColumn("col1", split(col("text"), " "))
        .map(x => (x,1) )
        .reduce(_._)


    Thread.sleep(args(0).toLong)
  }

}
