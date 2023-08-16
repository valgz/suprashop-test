package com.bluetab.example

import com.bluetab.example.config.SparkSessionInitializer
import com.bluetab.example.utils.ReadersAndWritersUtils.{readOracle, writeCommon, writeRaw}
import com.bluetab.example.utils.AppUtils._

import java.util.Calendar
//import com.bluetab.example.utils.DataFrameUtils.joinOrdersOrderItems

object MainApp extends App {

  val initSpark = SparkSessionInitializer.initSparkSession("Shop")
  implicit val spark = SparkSessionInitializer.completeSparkSession(initSpark)
  implicit val prop = loadPropertiesFile()

  val now = Calendar.getInstance()
  val year = now.get(Calendar.YEAR)
  val month = now.get(Calendar.MONTH)+1
  val day = now.get(Calendar.DAY_OF_MONTH)
  val hour = now.get(Calendar.HOUR)
  val minute = now.get(Calendar.MINUTE)
  println(s"year=$year month=$month day=$day hour=$hour minute=$minute")

  val dfControl = readOracle("control", "cargas")
  dfControl.show(false)

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

  writeRaw(dfOrderItems, year, month, day, hour, minute, prop.getProperty("pathRawOrderItems"))
  writeRaw(dfOrders, year, month, day, hour, minute, prop.getProperty("pathRawOrders"))
  writeRaw(dfCustomer, year, month, day, hour, minute, prop.getProperty("pathRawCustomer"))
  writeRaw(dfVendors, year, month, day, hour, minute, prop.getProperty("pathRawVendors"))
  writeRaw(dfProducts, year, month, day, hour, minute, prop.getProperty("pathRawProducts"))
/*
  writeCommon(dfOrderItems, prop.getProperty("pathCommonOrderItems"))
  writeCommon(dfOrders, prop.getProperty("pathCommonOrders"))
  writeCommon(dfCustomer, prop.getProperty("pathCommonCustomer"))
  writeCommon(dfVendors, prop.getProperty("pathCommonVendors"))
  writeCommon(dfProducts, prop.getProperty("pathCommonProducts"))
*/
  SparkSessionInitializer.stopSparkSession(spark)

}