package part3lowlevel

import common._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.File
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Date
import java.text.SimpleDateFormat
import scala.util.Using

object DStreams {

  val spark = SparkSession
    .builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /*
    Spark Streaming Context = entry point to the DStreams API
    - needs the spark context
    - a duration = batch interval
   */
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
    - define input sources by creating DStreams
    - define transformations on DStreams
    - call an action on DStreams
    - start ALL computations with ssc.start()
      - no more computations can be added
    - await termination, or stop the computation
      - you cannot restart the ssc
   */

  def readFromSocket() = {
    ssc
      .socketTextStream("localhost", 12345)
      // transformation = lazy
      .flatMap(_.split(" "))
      // action
      // wordsStream.print()
      // each folder = RDD = batch, each file = a partition of the RDD
      .saveAsTextFiles("src/main/resources/data/words/")

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile(path: String) =
    new Thread(() => {
      Thread.sleep(5000)

      val nFiles  = Files.list(Paths.get(path)).count()
      val newFile = new File(s"$path/newStocks$nFiles.csv")

      Using(new FileWriter(newFile)) { writer =>
        writer.write("""
          |AAPL,Sep 1 2000,12.88
          |AAPL,Oct 1 2000,9.78
          |AAPL,Nov 1 2000,8.25
          |AAPL,Dec 1 2000,7.44
          |AAPL,Jan 1 2001,10.81
          |AAPL,Feb 1 2001,9.12
        """.stripMargin.trim)
      }
    }).start()

  def readFromFile(stocksFilePath: String) = {
    createNewFile(stocksFilePath) // operates on another thread

    /*
      ssc.textFileStream monitors a directory for NEW FILES
     */
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    // transformations
    val dateFormat = new SimpleDateFormat("MMM d yyyy")

    val stocksStream: DStream[Stock] = textStream.map { line =>
      val tokens  = line.split(",")
      val company = tokens(0)
      val date    = new Date(dateFormat.parse(tokens(1)).getTime)
      val price   = tokens(2).toDouble

      Stock(company, date, price)
    }

    // action
    stocksStream.print()

    // start the computations
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit =
    readFromFile("src/main/resources/data/stocks")
}
