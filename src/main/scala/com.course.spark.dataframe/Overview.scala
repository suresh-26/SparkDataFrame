package com.course.spark.dataframe

// Getting Started with DataFrames!

// Most Important Link:
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset


// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
object Overview extends App {
  val spark = SparkSession.builder().getOrCreate()

  // Create a DataFrame from Spark Session read csv
  // Technically known as class Dataset
  val df = spark.read.option("header", "true").option("inferSchema", "true").csv("CitiGroup2006_2008")

  // Get first 5 rows
  df.head(5) // returns an Array
  println("\n")
  for (line <- df.head(10)) {
    println(line)
  }

  // Get column names
  println(df.columns)

  // Find out DataTypes
  // Print Schema
  df.printSchema()

  // Describe DataFrame Numerical Columns
  println(df.describe())

  // Select columns .transform().action()
  println(df.select("Volume").show())

  // Multiple Columns
  //TODO:: verify if it is ok to remove $ in below syntax, as before it was df.select($"Date", $"Close").show(2)
  df.select("Date", "Close").show(2)

  // Creating New Columns
  val df2 = df.withColumn("HighPlusLow", df("High") - df("Low"))
  // Show result
  df2.columns
  df2.printSchema()

  // Recheck Head
  df2.head(5)

  // Renaming Columns (and selecting some more)
  df2.select(df2("HighPlusLow").as("HPL"), df2("Close")).show()


  
  // That is it for now! We'll see these basic functions
  // a lot more as we go on.
}