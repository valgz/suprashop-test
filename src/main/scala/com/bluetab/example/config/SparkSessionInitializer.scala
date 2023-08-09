package com.bluetab.example.config

import org.apache.spark.sql.SparkSession

object SparkSessionInitializer {

  def initSparkSession(appName: String): SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName(appName)
      .getOrCreate()

  def completeSparkSession(sparkSession: SparkSession) = sparkSession

  def stopSparkSession(sparkSession: SparkSession): Unit = sparkSession.stop()

}
