package com.dionesh.uitilites

import org.apache.spark.sql.SparkSession

object SparkUtility {
  
    def createSparkSession(appName: String):SparkSession = SparkSession.builder.master("local[*]").appName(appName).getOrCreate()
    def readFile(spark: SparkSession, format: String, filePath: String) = spark.read.format(format).load(filePath)
}
