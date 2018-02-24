package com.spark.dataframe

import com.spark.dataframe.Operations.spark
import com.spark.dataframe.Overview.df
import org.apache.spark.sql.functions._
object MySolution extends App{
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().appName("Spark ml").config("spark.master", "local").getOrCreate()

  // Create a DataFrame from Spark Session read csv
  // Technically known as class Dataset
  val df = spark.read.option("header","true").option("inferSchema","true").csv("Netflix_2011_2016.csv")

  df.columns

  df.printSchema()

  df.show(5)

  df.describe().show()

  val df2 = df.withColumn("HVRatio", df("High") / df("Volume"))

  df2.show(5)

  df2.select(max("High")).show()

  df2.select(mean("Close")).show()

  df2.select(max("Volume")).show()

  df2.select(min("Volume")).show()

  import spark.implicits._

  val totalCount = df2.count()

  val lessThan600Count = df2.filter($"Close" < 600).count()

  println("total count: "+totalCount+ " and < 600: "+lessThan600Count)

  val greaterThan500Count = df2.filter($"Close" > 500).count

  println("total count: "+totalCount+ " and > 500: "+greaterThan500Count)

  val perGreaterThan500 = (greaterThan500Count.toDouble / totalCount.toDouble) * 100

  println("percent: "+ perGreaterThan500)

  df2.select(corr("High", "Volume")).show()

  df2.select(corr("Open", "High")).show()

  df2.groupBy(year(df("Date"))).max("High").orderBy("year(Date)").show()

  df2.groupBy(month(df("Date"))).mean("Close").show()

}
