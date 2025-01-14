package part2structuredstreaming

import common.ExtensionMethods._
import common.Schemas
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object StreamingDataFrames {

  val spark = SparkSession
    .builder()
    .appName("Our first streams")
    .master("local[2]")
    .getOrCreate()

  def readFromSocket() = {
    // reading a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformation
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 5)

    // tell between a static vs a streaming DF
    println(s"Are we streaming? ${shortLines.isStreaming.toYesNo}")

    // consuming a DF
    val query = shortLines.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()

    // wait for the stream to finish
    query.awaitTermination()
  }

  def readFromFiles() =
    spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(Schemas.stocks)
      .load("src/main/resources/data/stocks")
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()

  def demoTriggers() =
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      // write the lines DF at a certain trigger
      .trigger(
        // Trigger.ProcessingTime(2.seconds) // every 2 seconds run the query
        // Trigger.Once() // single batch, then terminate
        Trigger.Continuous(2.seconds) // experimental, every 2 seconds create a batch with whatever you have
      )
      .start()
      .awaitTermination()

  def main(args: Array[String]): Unit =
    readFromFiles()
}
