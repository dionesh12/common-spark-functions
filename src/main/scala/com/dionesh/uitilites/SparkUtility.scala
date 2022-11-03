package com.dionesh.uitilites

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrameReader

object SparkUtility {
  
    def createSparkSession(appName: String):SparkSession = SparkSession.builder.master("local[*]").appName(appName).getOrCreate()
    
    private def readFile(dataFrameReader: DataFrameReader, filePath: String) = dataFrameReader.load(filePath)


    def createDataFrame(spark: SparkSession,format: String, options: Map[String, String] = Map.empty, path: String)  = 
        readFile(spark.read.format(format).options(options), path)



}
