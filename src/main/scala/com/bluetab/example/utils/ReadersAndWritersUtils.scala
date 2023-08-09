package com.bluetab.example.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.bluetab.example.config.OracleConnect
import java.util.Properties

object ReadersAndWritersUtils {

  val oracleConnect = OracleConnect

  def readOracle(schema: String, dbtable: String)(implicit spark: SparkSession, prop: Properties): DataFrame = {
    val oracleConnectValues = oracleConnect.apply(schema)
    spark
      .read
      .format("jdbc")
      .option("url", oracleConnectValues.url)
      .option("user", oracleConnectValues.user)
      .option("password", oracleConnectValues.password)
      .option("dbtable", dbtable)
      .option("driver", oracleConnectValues.driver)
      .load()
  }

}