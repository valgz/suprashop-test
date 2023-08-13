package com.bluetab.example.utils

import org.apache.spark.sql.DataFrame

object DataFrameUtils {
  def joinOrdersOrderItems(dfOrders: DataFrame, dfOrderItems: DataFrame): DataFrame = {
    dfOrderItems.join(dfOrders, Seq("ORDER_ID"))
  }
}
