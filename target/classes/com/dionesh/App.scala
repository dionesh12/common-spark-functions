package com.dionesh

import org.apache.spark.sql.SparkSession
import com.dionesh.uitilites.SparkUtility.createSparkSession
/**
 * @author ${user.name}
 */
object App {
  
  def main(args : Array[String]):Unit = {
    val spark: SparkSession = createSparkSession("appName")
    println(spark)
  }

}
