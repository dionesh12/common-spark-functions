package com.dionesh

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions => F}
import com.dionesh.uitilites.SparkUtility.{createSparkSession, createDataFrame}
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.expressions.Window
import com.typesafe.config.ConfigFactory
/**
 * @author ${user.name}
 */

object App {

  val options = Map(
    "header" -> "true"
  )
  
  def main(args : Array[String]):Unit = {

    val spark: SparkSession = createSparkSession("appName")
    spark.sparkContext.setLogLevel("ERROR")
    val conf = ConfigFactory.load("etlConf.conf")

    val inputPath = conf.getString("input.path")
    val inputFormat =  conf.getString("input.format") 

    val outputPath = conf.getString("output.path")
    val outputFormat = conf.getString("output.format")
    val writeMode = conf.getString("output.mode")

    val eventsDF =  createDataFrame(spark, inputFormat, options, inputPath)
    
    val statsOfEventsPerItemPerDayDF =
      eventsDF
     .transform(addWeightedLabel)
     .transform(getEventPerCustomerPerItemId)
     .transform(convertTimeStampToDate)
     .transform(getAggregatedResult)

    statsOfEventsPerItemPerDayDF
    .repartition(2)
    .write
    .format(outputFormat)
    .mode(writeMode)
    .save(outputPath)

  }


  def  convertTimeStampToDate(eventsDF: DataFrame) = {
     eventsDF.withColumn("eventDate", F.to_date(F.from_unixtime(F.col("timestamp") /  1000)))
     .drop(F.col("timestamp"))
  }

  def getEventPerCustomerPerItemId(weightedEventsDF: DataFrame) = {
    val windowSpec = Window.partitionBy("timestamp", "visitorid", "itemid").orderBy(F.col("numericalEvent").asc)

    weightedEventsDF.withColumn("rowWeight", F.row_number().over(windowSpec))
    .filter(F.col("rowWeight") === 1)
    .drop("rowWeight")

  }

  def addWeightedLabel(eventsDF: DataFrame)  = {
    eventsDF.withColumn("numericalEvent", F.when(F.col("event") === "transaction", 0)
                                           .when(F.col("event") === "addtocart", 1)
                                           .when(F.col("event") === "view", 2)
                                           .otherwise(3))
  }

  def getAggregatedResult(eventsPerDateDF: DataFrame) = {

    val columnsToGroupBy = List(F.col("eventDate"), F.col("itemid"), F.col("event"))
    val columnsToGroupForPivot = List(F.col("eventDate"), F.col("itemid"))  
    
     eventsPerDateDF
     .groupBy(columnsToGroupBy: _*)
      .agg(F.count("*").as("count"))
      .groupBy(columnsToGroupForPivot: _*)
      .pivot(F.col("event"))
      .agg(F.sum("count"))
      .na.fill(0)
  }
  
}
