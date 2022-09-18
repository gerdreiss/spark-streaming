package part4integrations

import common._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object IntegratingKafka {

  val spark = SparkSession
    .builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

  def readFromKafka(): Unit =
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()

  def writeToKafka(): Unit =
    spark.readStream
      .schema(Schemas.cars)
      .json("src/main/resources/data/cars")
      .selectExpr("upper(Name) as key", "Name as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") // without checkpoints the writing to Kafka will fail
      .start()
      .awaitTermination()

  /**
    * Exercise: write the whole cars data structures to Kafka as JSON.
    * Use struct columns an the to_json function.
    */
  def writeCarsToKafka(): Unit =
    spark.readStream
      .schema(Schemas.cars)
      .json("src/main/resources/data/cars")
      .select(
        col("Name").as("key"),
        to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("String").as("value"),
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()

  def main(args: Array[String]): Unit =
    writeCarsToKafka()

}
