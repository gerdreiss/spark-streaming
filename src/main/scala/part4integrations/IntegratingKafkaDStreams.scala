package part4integrations

import common.ExtensionMethods._
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import scala.util.Using

object IntegratingKafkaDStreams {

  val spark = SparkSession
    .builder()
    .appName("Spark DStreams + Kafka")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers"  -> "localhost:9092",
    "key.serializer"     -> classOf[StringSerializer],   // send data to kafka
    "value.serializer"   -> classOf[StringSerializer],
    "key.deserializer"   -> classOf[StringDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset"  -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object],
  )

  val kafkaTopic = "rockthejvm"

  def readFromKafka(): Unit = {
    KafkaUtils
      .createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        /*
         * Distributes the partitions evenly across the Spark cluster.
         * Alternatives:
         * - PreferBrokers if the brokers and executors are in the same cluster
         * - PreferFixed
         */
        ConsumerStrategies.Subscribe[String, String](Array(kafkaTopic), kafkaParams + ("group.id" -> "group1")),
        /*
         * Alternative
         * - SubscribePattern allows subscribing to topics matching a pattern
         * - Assign - advanced; allows specifying offsets and partitions per topic
         */
      )
      .map(record => (record.key(), record.value()))
      .print()

    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka(): Unit = {
    ssc
      .textFileStream("src/main/resources/data/lipsum")
      // transform data
      .map(_.toUpperCase())
      .foreachRDD {
        _.foreachPartition { partition =>
          // inside this lambda, the code is run by a single executor
          // producer can insert records into the Kafka topics
          // available on this executor
          Using(new KafkaProducer[String, String](kafkaParams.toJavaHashMap)) { producer =>
            partition.foreach { record =>
              producer.send(new ProducerRecord[String, String](kafkaTopic, record))
            }
          }
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit =
    writeToKafka()
}
