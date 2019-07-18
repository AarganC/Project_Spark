package io.saagie.esgi.spark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger


object SparkFinalProject {

  case class Beer(id: Int, name: String, brewery_id: String, state: String, country: String, style: String, availability: String, abv: String, notes: String, retired: String)
  case class Breweries(id: Int, name: String, city: String, state: String, country: String, notes: String , types: String)
  case class Review(beer_id: Int, username: String, date: String, text: String, look: String, smell: String, taste: String, feel: String, overall: String, score: String)
  case class StopWords(text: String)

  //lazy val log = Logger.getLogger(this.getClass.getName)


  def main(args: Array[String]) {
    val PATH = "data/beers-breweries-and-beer-reviews"

    val spark = SparkSession.builder()
      .appName("Final-Project-Spark-G4")
      .getOrCreate()

    import spark.implicits._


    val beer = spark
      .read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"$PATH/beers.csv")
      //.createOrReplaceTempView("beer")
    val dsBeer: Dataset[Beer] = beer.as[Beer]

    val breweries = spark
      .read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"$PATH/breweries.csv")
      //.createOrReplaceTempView("breweries")
    val dsBreweries: Dataset[Breweries] = breweries.as[Breweries]

    val reviews = spark
      .read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"$PATH/reviews.csv")
      //.createOrReplaceTempView("reviews")
    val dsReview: Dataset[Review] = reviews.as[Review]

    val max_avg_beer = dsReview.groupBy("beer_id")
      .agg(avg("score").alias("avg_score"))
      .withColumnRenamed("beer_id", "id")
      .join(dsBeer, "id")
      .select("name", "avg_score")
      .orderBy(desc("avg_score"))

    val max_avg_breweries = dsReview.withColumnRenamed("beer_id", "id")
      .join(dsBreweries, "id")
      .groupBy("name")
      .agg(avg("score").alias("avg_score"))
      .select("name", "avg_score")
      .orderBy(desc("avg_score"))

    val top_breweries = dsBreweries.filter($"types".contains("Beer-to-go"))
      .groupBy("country")
      .count
      .orderBy(desc("count"))

    max_avg_beer.select("Select name, MAX(avg_score)").show
    max_avg_breweries.select("Select name, MAX(avg_score)").show
    top_breweries.show(10)
    //log.info(top_breweries.show(10))
  }

}
