package part3lowlevel

import common.Models._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Date
import java.time.LocalDate
import java.time.Period

object DStreamsTransformations {

  val spark = SparkSession
    .builder()
    .appName("DStreams Transformations")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

  import spark.implicits._ // for encoders to create Datasets

  def readPeople: DStream[Person] =
    ssc
      .textFileStream("src/main/resources/data/people-1m")
      .map { line =>
        val tokens = line.split(":")
        Person(
          tokens(0).toInt,         // id
          tokens(1),               // first name
          tokens(2),               // middle name
          tokens(3),               // last name
          tokens(4),               // gender
          Date.valueOf(tokens(5)), // birth
          tokens(6),               // ssn/uuid
          tokens(7).toInt,         // salary
        )
      }
      .cache()

  // map, flatMap, filter
  def peopleAges: DStream[(String, Int)] =
    readPeople.map { person =>
      val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
      (s"${person.firstName} ${person.lastName}", age)
    }

  def peopleSmallNames: DStream[String] =
    readPeople.flatMap { person =>
      List(person.firstName, person.middleName)
    }

  def highIncomePeople: DStream[Person] =
    readPeople.filter(_.salary > 80000)

  // count
  def countPeople: DStream[Long] =
    readPeople.count() // the number of entries in every batch

  // count by value, PER BATCH
  def countNames: DStream[(String, Long)] =
    readPeople
      .map(_.firstName)
      .countByValue()

  /*
   reduce by key
   - works on DStream of tuples
   - works PER BATCH
   */
  def countNamesReduce: DStream[(String, Long)] =
    readPeople
      .map(_.firstName)
      // .map((_, 1L))
      // .reduceByKey(_ + _)
      .countByValue(4)

  // foreach rdd
  def saveToJson() =
    readPeople.foreachRDD { rdd =>
      val path    = "src/main/resources/data/people-1m/"
      val nFiles  = Files.list(Paths.get(path)).count()
      val newFile = s"$path$nFiles.json"

      spark.createDataset(rdd).write.json(newFile)
    }

  def main(args: Array[String]): Unit = {
    peopleAges.print(10)
    peopleSmallNames.print(10)
    highIncomePeople.print(10)
    countPeople.print(10)
    countNames.print(10)
    countNamesReduce.print(10)
    saveToJson()

    ssc.start()
    ssc.awaitTermination()
  }

}
