package com.bluetab.example

import com.bluetab.example.config.SparkSessionInitializer
import com.bluetab.example.utils.ReadersAndWritersUtils.{readOracle, writeCommon, writeRaw, writeStage}
import com.bluetab.example.utils.AppUtils._
import com.bluetab.example.utils.LoadUtils.readFechaCarga
import org.apache.spark.sql.functions.col

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
  //dfControl.show(false)

  /*************************Lecturas incrementales***********************/

  val fechaControlOrderItems = readFechaCarga(dfControl, "order_items")
  val fechaControlOrders = readFechaCarga(dfControl, "orders")
  val fechaControlCustomer = readFechaCarga(dfControl, "customer")
  val fechaControlVendors = readFechaCarga(dfControl, "vendors")
  val fechaControlProducts = readFechaCarga(dfControl, "products")

  val newOrderItems = readOracle("shop", "order_items").filter(col("update_date") > fechaControlOrderItems)
  val newOrders = readOracle("shop", "orders").filter(col("update_date") > fechaControlOrders)
  val newCustomer = readOracle("shop", "customer").filter(col("update_date") > fechaControlCustomer)
  val newVendors = readOracle("shop", "vendors").filter(col("update_date") > fechaControlVendors)
  val newProducts = readOracle("shop", "products").filter(col("update_date") > fechaControlProducts)

  /***********************Escritura en STAGE*************************/

  if (!newOrderItems.isEmpty) writeStage(newOrderItems, prop.getProperty("pathStageOrderItems"))
  if (!newOrders.isEmpty) writeStage(newOrders, prop.getProperty("pathStageOrders"))
  if (!newCustomer.isEmpty) writeStage(newCustomer, prop.getProperty("pathStageCustomer"))
  if (!newOrderItems.isEmpty) writeStage(newVendors, prop.getProperty("pathStageVendors"))
  if (!newProducts.isEmpty) writeStage(newProducts, prop.getProperty("pathStageProducts"))

  /*
  val dfOrdersItemsJoin = joinOrdersOrderItems(dfOrders, dfOrderItems)
  dfOrders.show(false)
  dfOrderItems.show(false)
  dfOrdersItemsJoin.show(false)
*/
  /*******************************Lectura completa*******************************/

  val dfOrderItems = readOracle("shop", "order_items")
  val dfOrders = readOracle("shop", "orders")
  val dfCustomer = readOracle("shop", "customer")
  val dfVendors = readOracle("shop", "vendors")
  val dfProducts = readOracle("shop", "products")

  /***************************Escritura en RAW y COMMON********************************/

   writeRaw(dfOrderItems, year, month, day, hour, minute, prop.getProperty("pathRawOrderItems"))
   writeRaw(dfOrders, year, month, day, hour, minute, prop.getProperty("pathRawOrders"))
   writeRaw(dfCustomer, year, month, day, hour, minute, prop.getProperty("pathRawCustomer"))
   writeRaw(dfVendors, year, month, day, hour, minute, prop.getProperty("pathRawVendors"))
   writeRaw(dfProducts, year, month, day, hour, minute, prop.getProperty("pathRawProducts"))

   writeCommon(dfOrderItems, year, month, day, hour, minute, prop.getProperty("pathCommonOrderItems"))
   writeCommon(dfOrders, year, month, day, hour, minute, prop.getProperty("pathCommonOrders"))
   writeCommon(dfCustomer, year, month, day, hour, minute, prop.getProperty("pathCommonCustomer"))
   writeCommon(dfVendors, year, month, day, hour, minute, prop.getProperty("pathCommonVendors"))
   writeCommon(dfProducts, year, month, day, hour, minute, prop.getProperty("pathCommonProducts"))

  SparkSessionInitializer.stopSparkSession(spark)

}