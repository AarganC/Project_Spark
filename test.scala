

//val conf = new SparkConf().setAppName("Final-Project-Spark-G4").setMaster("local[*]")
//val sc = new SparkContext(conf)


/*val schema_beer = StructType(Array(
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
  .createOrReplaceTempView("beer")*/

/*val schema_breweries = StructType(Array(
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
  .createOrReplaceTempView("breweries")*/

/*val schema_reviews = StructType(Array(
  StructField("id", IntegerType),
  StructField("username", StringType),
  StructField("date", DateType),
  StructField("text", StringType),
  StructField("look", FloatType),
  StructField("smell", FloatType),
  StructField("taste", FloatType),
  StructField("feel", FloatType),
  StructField("overall", FloatType),
  StructField("score", FloatType)
))

val reviews = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .schema(schema_reviews)
  .csv(s"$PATH/reviews.csv")
  .select("id", "score", "text")
  .createOrReplaceTempView("reviews")*/



/*beer.join(breweries, usingColumn = "id")
    .join(reviews, usingColumn = "id")
    .select("id", "country", "name", "types", "score", "text", "brewery_id")
    .createOrReplaceTempView("tmp_data")*/

//val valuedf = spark.sql("SELECT * FROM reviews LIMIT 5")
//valuedf.show

//val max_score = spark.sql("SELECT beer_id,  SUM(score)/COUNT(*) AS avg_score  FROM reviews  GROUP BY beer_id ORDER BY avg_score DESC")
//val max_score = spark.sql("SELECT beer.name, tmp.avg_score FROM (SELECT beer_id, AVG(score) AS avg_score FROM reviews GROUP BY beer_id) as tmp JOIN beer ON beer.id = tmp.beer_id ORDER BY tmp.avg_score DESC")

/*val max_score = reviews.groupBy("beer_id")
  .agg(avg("score").alias("avg_score"))
  //.join(beer, on, "left")
  //.select("name", "avg_score")
  .orderBy(desc("avg_score"))*/


//val max_score = spark.sql("SELECT distinct(tmp.avg_score) FROM (SELECT id, AVG(score) AS avg_score FROM reviews GROUP BY id) as tmp JOIN beer ON beer.id = tmp.id ORDER BY tmp.avg_score DESC")
//val max_avg = spark.sql("SELECT name, AVG(score) as avg_score FROM tmp_data ")
//val max_avg = spark.sql("SELECT MAX(avg_score) FROM (SELECT name, AVG(score) as avg_score FROM tmp_data GROUP BY name)")
//val top_type = spark.sql("SELECT brewery_id, types, count(brewery_id) as count_brasserie FROM tmp_data WHERE types like '%Beer-to-go%' GROUP BY brewery_id, types ORDER BY count(brewery_id) ASC LIMIT 10")
