package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingJoins {

  val spark = SparkSession
    .builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .getOrCreate()

  val guitarPlayers = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers")
    .cache()

  val guitars = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars")
    .cache()

  val bands = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands")
    .cache()

  // schemas
  val guitarPlayersSchema = guitarPlayers.schema
  val bandsSchema         = bands.schema

  // joining static DFs
  val joinCondition   = guitarPlayers.col("band") === bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joinCondition, "inner")

  def joinStreamWithStatic() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // join happens PER BATCH
    val halfStreamedJoinCondition = guitarPlayers.col("band") === streamedBandsDF.col("id")
    val streamedBandsGuitaristsDF = streamedBandsDF.join(guitarPlayers, halfStreamedJoinCondition, "inner")

    /*
      restricted joins:
      - stream joining with static: RIGHT outer join/full outer join/right_semi not permitted
      - static joining with streaming: LEFT outer join/full/left_semi not permitted
     */

    streamedBandsGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // since Spark 2.3 we have stream vs stream joins
  def joinStreamWithStream() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr(
        "band.id as id",
        "band.name as name",
        "band.hometown as hometown",
        "band.year as year",
      )

    val streamedGuitaristsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(from_json(col("value"), guitarPlayersSchema).as("guitarPlayer"))
      .selectExpr(
        "guitarPlayer.id as id",
        "guitarPlayer.name as name",
        "guitarPlayer.guitars as guitars",
        "guitarPlayer.band as band",
      )

    // join stream with stream
    val streamedJoinCondition        = streamedGuitaristsDF.col("band") === streamedBandsDF.col("id")
    val streamedBandsAndGuitaristsDF = streamedBandsDF.join(streamedGuitaristsDF, streamedJoinCondition)

    /*
      - inner joins are supported
      - left/right outer joins ARE supported, but MUST have watermarks
      - full outer joins are NOT supported
     */

    streamedBandsAndGuitaristsDF.writeStream
      .format("console")
      .outputMode("append") // only append supported for stream vs stream join
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit =
    joinStreamWithStream()

}
