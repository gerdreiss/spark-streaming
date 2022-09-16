package part4integrations

import common.ExtensionMethods._
import common.Models._
import common.Schemas
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

object IntegratingJDBC {

  val spark = SparkSession
    .builder()
    .appName("Integrating JDBC")
    .master("local[2]")
    .getOrCreate()

  val driver   = "org.postgresql.Driver"
  val url      = "jdbc:postgresql://localhost:5432/rtjvm"
  val user     = "docker"
  val password = "docker"

  import spark.implicits._

  def writeStreamToPostgres() =
    spark.readStream
      .schema(Schemas.cars)
      .json("src/main/resources/data/cars")
      .as[Car]
      .writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // each executor can control the batch
        // batch is a STATIC Dataset/DataFrame

        // batch.write
        //   .format("jdbc")
        //   .option("driver", driver)
        //   .option("url", url)
        //   .option("user", user)
        //   .option("password", password)
        //   .option("dbtable", "public.cars")
        //   .save()

        batch.write.jdbc(
          url,
          "public.cars",
          Map(
            "driver"   -> driver,
            "user"     -> user,
            "password" -> password,
          ).toJavaProperties,
        )
      }
      .start()
      .awaitTermination()

  def main(args: Array[String]): Unit =
    writeStreamToPostgres()

}
