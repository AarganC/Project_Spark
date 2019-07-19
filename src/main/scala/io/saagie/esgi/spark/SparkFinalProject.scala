package io.saagie.esgi.spark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object SparkFinalProject {

  case class Beer(id: Int, name: String, brewery_id: String, state: String, country: String, style: String, availability: String, abv: String, notes: String, retired: String)
  case class Breweries(id: Int, name: String, city: String, state: String, country: String, notes: String , types: String)
  case class Review(beer_id: Int, username: String, date: String, text: String, look: String, smell: String, taste: String, feel: String, overall: String, score: String)
  case class StopWords(value: String)


  def main(args: Array[String]) {
    val PATH = "hdfs:/data/esgi-spark/final-project"

    val spark = SparkSession.builder()
      .appName("Final-Project-Spark-G4")
      .getOrCreate()

    import spark.implicits._

    val stopWords = spark.read.text(s"$PATH/common-english-words.txt")
      .flatMap(row => row.getString(0)
      .split(","))

    val dsStopWords: Dataset[StopWords] = stopWords.as[StopWords]

    val beer = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"$PATH/beers.csv")

    val dsBeer: Dataset[Beer] = beer.as[Beer]

    val breweries = spark
      .read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"$PATH/breweries.csv")

    val dsBreweries: Dataset[Breweries] = breweries.as[Breweries]

    val reviews = spark
      .read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"$PATH/reviews 2.csv")

    val dsReview: Dataset[Review] = reviews.as[Review]

    print("Best scored beer")

    val max_avg_beer = dsReview.groupBy("beer_id")
      .agg(avg("score").alias("avg_score"))
      .withColumnRenamed("beer_id", "id")
      .join(dsBeer, "id")
      .select("name", "avg_score")
      .orderBy(desc("avg_score"))
      .show(1)

    print("Best scored brewery")


    val max_avg_breweries = dsReview.withColumnRenamed("beer_id", "id")
      .join(dsBreweries, "id")
      .groupBy("name")
      .agg(avg("score").alias("avg_score"))
      .select("name", "avg_score")
      .orderBy(desc("avg_score"))
      .show(1)

    val top_breweries = dsBreweries.filter($"types".contains("Beer-to-go"))
      .groupBy("country")
      .count
      .orderBy(desc("count"))

    print("10 Best scored countries that have the most beer-to-go breweries")

    top_breweries.show(10)


    print("Words occurencies in reviews")

    val reviewIPA = dsBeer
      .filter($"style".contains("IPA"))
      .join(
        dsReview,
        dsReview("beer_id") === dsBeer("id"),
        "inner")
      .as[Review]
      .map(_.text)
      .map(_.replaceAll("""[\p{Punct}&&[^.]]""", ""))
      .flatMap(_.toLowerCase().split(" "))
      .groupBy("value").count()
      .orderBy($"count".desc)
      .withColumnRenamed("value", "word")
      .join(
        dsStopWords,
        dsStopWords("value") === $"word",
        "leftanti"
      )
      .show(10)

    Thread.sleep(args(0).toLong)
  }

}
