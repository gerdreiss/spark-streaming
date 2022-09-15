package common

import java.sql.Date

object Models {

  case class Car(
      Name: String,
      Miles_per_Gallon: Option[Double],
      Cylinders: Option[Long],
      Displacement: Option[Double],
      Horsepower: Option[Long],
      Weight_in_lbs: Option[Long],
      Acceleration: Option[Double],
      Year: String,
      Origin: String,
  )

  case class Person(
      id: Int,
      firstName: String,
      middleName: String,
      lastName: String,
      gender: String,
      birthDate: Date,
      ssn: String,
      salary: Int,
  )

  case class Stock(
      company: String,
      date: Date,
      value: Double,
  )

}
