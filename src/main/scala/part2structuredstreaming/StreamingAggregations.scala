package part2structuredstreaming

import org.apache.spark.sql.{ Column, DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StreamingAggregations {

  val spark = SparkSession
    .builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  def streamingCount() =
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      // aggregations with distinct are not supported
      // otherwise Spark will need to keep track of EVERYTHING
      .selectExpr("count(*) as lineCount")
      .writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()

  def numericalAggregations(aggFunction: Column => Column): Unit =
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      // aggregate here
      .select(col("value").cast(IntegerType).as("number"))
      .select(aggFunction(col("number")).as("agg_so_far"))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  def groupNames(): Unit =
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      // counting occurrences of the "name" value
      .select(col("value").as("name"))
      .groupBy(col("name")) // RelationalGroupedDataset
      .count()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  def main(args: Array[String]): Unit =
    // streamingCount()
    // groupNames()
    numericalAggregations(sum)

}
