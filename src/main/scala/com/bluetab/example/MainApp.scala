package com.bluetab.example

import com.bluetab.example.config.SparkSessionInitializer
import com.bluetab.example.utils.ReadersAndWritersUtils.readOracle
import com.bluetab.example.utils.AppUtils._

object MainApp extends App {

  val initSpark = SparkSessionInitializer.initSparkSession("Shop")
  implicit val spark = SparkSessionInitializer.completeSparkSession(initSpark)
  implicit val prop = loadPropertiesFile()

  val dfPedido = readOracle("shop", "products")
  dfPedido.show(false)

  SparkSessionInitializer.stopSparkSession(spark)

}