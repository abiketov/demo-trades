package com.demo.analytics

import java.util.UUID


import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.Try

object Trades {

  case class Event(tradeId:String, product: String, date: String, amount: Double)

  def main(args: Array[String]): Unit = {



    Logger.getLogger("org").setLevel(Level.ERROR)

    val appConfig = ConfigFactory.load("application.conf")
    val defaultFileName = appConfig.getString("file.name")
    val defaultFilePath = appConfig.getString("file.path")

    val sparkMaster = Option(System.getProperty("spark.master")).getOrElse("local")
    val fileName = Try(args(0)).toOption.getOrElse(defaultFileName)
    val filePath = Try(args(1)).toOption.getOrElse(defaultFilePath)


    val conf = new SparkConf(true)
      .set("spark.master",sparkMaster)

    val spark: SparkSession = SparkSession.builder()
      .appName("Trades analytics")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val rdd: RDD[String] = spark.sparkContext.textFile(s"$filePath/$fileName")

    val events = rdd.map(t => t.split("\\|").toList)
        .flatMap(e => {
          val id = UUID.randomUUID().toString
          e.map(q => {
            val s = q.split(",")
            Event(id, s(0).trim, s(1).trim, s(2).trim.toDouble)
          })
        })

    val tradesDF = events.toDF()
    tradesDF.cache()

    tradesDF.show()

    //1.	What are the total number of events?
    //println(s"Total number of events:${tradesDF.count()}")
    println("What are the total number of events?")
    tradesDF.select(col("product")).agg(count("product")).show()

    //2.	What are the distinct product types?
    println("What are the distinct product types?")
    tradesDF.select(col("product")).distinct().show()

    //3.	For each product type, what is the total amount?
    println("For each product type, what is the total amount?")
    tradesDF.groupBy(col("product")).agg(sum(col("amount"))).show()

    //4.	Can we see the events ordered by the time they occurred?
    println("Can we see the events ordered by the time they occurred?")
    tradesDF.select($"tradeId", $"product", $"date", $"amount").orderBy($"date").show()

    //6.	How do we get the minimum amount for each product type?
    //7.	How do we get the maximum amount for each product type?
    println("How do we get the minimum amount for each product type?")
    println("How do we get the maximum amount for each product type?")

    tradesDF.groupBy(col("product"))
            .agg(min("amount"),max("amount") ).show()

    //8.	How do we get the total amount for each trade, and the max/min across trades?
    println("How do we get the total amount for each trade, and the max/min across trades?")
    tradesDF.groupBy(col("tradeId"))
            .agg(format_number(sum(col("amount")),2).alias("total"))
            .show()

    val tempDF = tradesDF.groupBy(col("tradeId"))
      .agg(sum(col("amount")).alias("total")).toDF()
      tempDF.agg(format_number(max("total"),2).alias("max total"),
                 format_number(min("total"),2).alias("min total")).show()

    //5.	Data needs to be saved into Hive for other consumers.
    println("Data needs to be saved into Hive for other consumers.")
    tradesDF.write.format("ORC").mode("overwrite").save(s"$filePath/trades.orc")
    tradesDF.write.format("csv").mode("overwrite").save(s"$filePath/trades.csv")
  }
}
