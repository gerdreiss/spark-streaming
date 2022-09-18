package part7science

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

object ScienceServer {
  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val kafkaTopic           = "science"
  val kafkaBootstrapServer = "localhost:9092"

  def getProducer() = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "MyKafkaProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    new KafkaProducer[Long, String](props)
  }

  def getRoute(producer: KafkaProducer[Long, String]) =
    pathEndOrSingleSlash {
      get {
        complete(
          HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            Source.fromFile("src/main/html/whackamole.html").getLines().mkString(""),
          )
        )
      }
    } ~
      path("api" / "report") {
        (parameter("sessionId".as[String]) & parameter("time".as[Long])) { (sessionId: String, time: Long) =>
          println(s"I've found session ID $sessionId and time = $time")

          // create a record to send to kafka
          val record = new ProducerRecord[Long, String](kafkaTopic, 0, s"$sessionId,$time")
          producer.send(record)
          producer.flush()

          complete(StatusCodes.OK) // HTTP 200
        }
      }

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    // spinning up the server
    val kafkaProducer = getProducer()
    val bindingFuture = Http().bindAndHandle(getRoute(kafkaProducer), "localhost", 9988)

    // cleanup
    bindingFuture.foreach { binding =>
      binding.whenTerminated.onComplete(_ => kafkaProducer.close())
    }
  }

}
