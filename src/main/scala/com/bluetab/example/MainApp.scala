package com.bluetab.example

import com.bluetab.example.config.SparkSessionInitializer
import com.bluetab.example.utils.ReadersAndWritersUtils.{readOracle, writeCommon, writeRaw}
import com.bluetab.example.utils.AppUtils._
//import com.bluetab.example.utils.DataFrameUtils.joinOrdersOrderItems

object MainApp extends App {

  val initSpark = SparkSessionInitializer.initSparkSession("Shop")
  implicit val spark = SparkSessionInitializer.completeSparkSession(initSpark)
  implicit val prop = loadPropertiesFile()

  val dfOrderItems = readOracle("shop", "order_items")
  val dfOrders = readOracle("shop", "orders")
  val dfCustomer = readOracle("shop", "customer")
  val dfVendors = readOracle("shop", "vendors")
  val dfProducts = readOracle("shop", "products")

  /*
  val dfOrdersItemsJoin = joinOrdersOrderItems(dfOrders, dfOrderItems)
  dfOrders.show(false)
  dfOrderItems.show(false)
  dfOrdersItemsJoin.show(false)
  */

  writeRaw(dfOrderItems, prop.getProperty("pathRawOrderItems"))
  writeRaw(dfOrders, prop.getProperty("pathRawOrders"))
  writeRaw(dfCustomer, prop.getProperty("pathRawCustomer"))
  writeRaw(dfVendors, prop.getProperty("pathRawVendors"))
  writeRaw(dfProducts, prop.getProperty("pathRawProducts"))

  writeCommon(dfOrderItems, prop.getProperty("pathCommonOrderItems"))
  writeCommon(dfOrders, prop.getProperty("pathCommonOrders"))
  writeCommon(dfCustomer, prop.getProperty("pathCommonCustomer"))
  writeCommon(dfVendors, prop.getProperty("pathCommonVendors"))
  writeCommon(dfProducts, prop.getProperty("pathCommonProducts"))

  SparkSessionInitializer.stopSparkSession(spark)

}