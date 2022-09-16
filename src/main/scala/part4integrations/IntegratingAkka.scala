package part4integrations

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import common.Models._
import common.Schemas
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

object IntegratingAkka {

  val spark = SparkSession
    .builder()
    .appName("Integrating Akka")
    .master("local[2]")
    .getOrCreate()

  // foreachBatch
  // receiving system is on another JVM

  import spark.implicits._

  def writeCarsToAkka(): Unit = {
    val akkaConfig = ConfigFactory.load("akkaconfig/remoteActors")

    spark.readStream
      .schema(Schemas.cars)
      .json("src/main/resources/data/cars")
      .as[Car]
      .writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        batch.foreachPartition { cars: Iterator[Car] =>
          // this code is run by a single executor

          val system     = ActorSystem(s"SourceSystem$batchId", akkaConfig)
          val entryPoint = system.actorSelection("akka://ReceiverSystem@localhost:2552/user/entrypoint")

          // send all the data
          cars.foreach(car => entryPoint ! car)
        }
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit =
    writeCarsToAkka()
}

object ReceiverSystem {
  private val akkaConfig = ConfigFactory.load("akkaconfig/remoteActors")

  implicit val actorSystem       = ActorSystem("ReceiverSystem", akkaConfig.getConfig("remoteSystem"))
  implicit val actorMaterializer = ActorMaterializer()

  class Destination extends Actor with ActorLogging {
    override def receive = { case m =>
      log.info(m.toString)
    }
  }

  class EntryPoint(destination: ActorRef) extends Actor with ActorLogging {
    override def receive = { case m =>
      log.info(s"Received $m")
      destination ! m
    }
  }

  object EntryPoint {
    def props(destination: ActorRef) = Props(new EntryPoint(destination))
  }

  def main(args: Array[String]): Unit = {
    val source                = Source.actorRef[Car](bufferSize = 10, overflowStrategy = OverflowStrategy.dropHead)
    val sink                  = Sink.foreach[Car](println)
    val runnableGraph         = source.to(sink)
    val destination: ActorRef = runnableGraph.run()

    val entryPoint = actorSystem.actorOf(EntryPoint.props(destination), "entrypoint")
  }
}
