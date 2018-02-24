package com.spark.dataframe

import org.apache.spark.sql.SparkSession

// Most Important Link:
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset
object Overview extends App {

  // Start a simple Spark Session
  val spark = SparkSession.builder().appName("Spark ml").config("spark.master", "local").getOrCreate()

  // Create a DataFrame from Spark Session read csv
  // Technically known as class Dataset
  val df = spark.read.option("header", "true").option("inferSchema", "true").csv("citiData_2006-08")

  // Get first 5 rows
  df.head(5) // returns an Array
  println("\n")
  for (line <- df.head(10)) {
    println(line)
  }

  // Get column names
  val columns = df.columns

  // Find out DataTypes
  // Print Schema
  df.printSchema()

  // Describe DataFrame Numerical Columns
  df.describe().show()

  // Select columns .transform().action()
  df.select("Volume").show()

  // Multiple Columns
  df.select("Date", "Close").show(2)

  // Creating New Columns
  val df2 = df.withColumn("HighPlusLow", df("High") - df("Low"))
  // Show result
  df2.columns
  df2.printSchema()

  // Recheck Head
  for (line <- df2.head(5)) {
    println(line)
  }

  // Renaming Columns (and selecting some more)
  df2.select(df2("HighPlusLow").as("HPL"), df2("Close")).show()

}