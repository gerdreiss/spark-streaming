package common

import org.apache.spark.sql.types._

object Schemas {

  val cars: StructType = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType),
    )
  )

  val stocks: StructType = StructType(
    Array(
      StructField("company", StringType),
      StructField("date", DateType),
      StructField("value", DoubleType),
    )
  )

  val onlinePurchase = StructType(
    Array(
      StructField("id", StringType),
      StructField("time", TimestampType),
      StructField("item", StringType),
      StructField("quantity", IntegerType),
    )
  )
}
