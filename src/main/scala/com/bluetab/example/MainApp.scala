package com.bluetab.example

import com.bluetab.example.config.SparkSessionInitializer
import com.bluetab.example.utils.ReadersAndWritersUtils.{readOracle, writeRaw}
import com.bluetab.example.utils.AppUtils._

object MainApp extends App {

  val initSpark = SparkSessionInitializer.initSparkSession("Shop")
  implicit val spark = SparkSessionInitializer.completeSparkSession(initSpark)
  implicit val prop = loadPropertiesFile()

  val dfOrderItems = readOracle("shop", "order_items")
  val dfOrders = readOracle("shop", "orders")
  val dfCustomer = readOracle("shop", "customer")
  val dfVendors = readOracle("shop", "vendors")
  val dfProducts = readOracle("shop", "products")

  //val dfOrdersItemsJoin = joinOrdersOrderItems(dfOrders, dfOrderItems)

  //dfOrdersItemsJoin.show(false)

  writeRaw(dfOrderItems, prop.getProperty("pathRawOrder_Items"))
  writeRaw(dfOrders, prop.getProperty("pathRawOrders"))
  writeRaw(dfCustomer, prop.getProperty("pathRawCustomer"))
  writeRaw(dfVendors, prop.getProperty("pathRawVendors"))
  writeRaw(dfProducts, prop.getProperty("pathRawProducts"))

  SparkSessionInitializer.stopSparkSession(spark)

}