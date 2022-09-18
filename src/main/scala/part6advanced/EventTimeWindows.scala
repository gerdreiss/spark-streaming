package part6advanced

import common.Schemas
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object EventTimeWindows {

  val spark = SparkSession
    .builder()
    .appName("Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  def readPurchasesFromSocket(): DataFrame =
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), Schemas.onlinePurchase).as("purchase"))
      .selectExpr("purchase.*")

  def readPurchasesFromFile(): DataFrame =
    spark.readStream
      .schema(Schemas.onlinePurchase)
      .json("src/main/resources/data/purchases")

  def aggregatePurchasesByTumblingWindow(): Unit =
    readPurchasesFromFile()
      .groupBy(window(col("time"), "1 day").as("time")) // tumbling window: sliding duration == window duration
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity"),
      )
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination(16000)

  def aggregatePurchasesBySlidingWindow(): Unit =
    readPurchasesFromFile()
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // struct column: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity"),
      )
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination(16000)

  /**
    * Exercises
    * 1) Show the best selling product of every day, +quantity sold.
    * 2) Show the best selling product of every 24 hours, updated every hour.
    */

  def bestSellingProductPerDay() =
    readPurchasesFromFile()
      .groupBy(col("item"), window(col("time"), "1 day").as("day"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("day").getField("start").as("start"),
        col("day").getField("end").as("end"),
        col("item"),
        col("totalQuantity"),
      )
      .orderBy(col("day"), col("totalQuantity").desc)
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination(16000)

  def bestSellingProductEvery24h() =
    readPurchasesFromFile()
      .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("time"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("item"),
        col("totalQuantity"),
      )
      .orderBy(col("start").desc, col("totalQuantity").desc)
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination(16000)

  /*
    For window functions, windows start at Jan 1 1970, 12 AM GMT
   */

  def main(args: Array[String]): Unit = {
    aggregatePurchasesByTumblingWindow()
    aggregatePurchasesBySlidingWindow()
    bestSellingProductPerDay()
    bestSellingProductPerDay()
  }

}
