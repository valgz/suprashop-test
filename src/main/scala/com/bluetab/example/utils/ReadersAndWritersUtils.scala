package com.bluetab.example.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.bluetab.example.config.OracleConnect
import org.apache.spark.sql.functions.{col, dayofmonth, hour, minute, month, year}
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

  def writeRaw(dfData: DataFrame, path: String)(implicit spark: SparkSession, prop: Properties): Unit = {
    dfData
      .withColumn("year", year(col("UPDATE_DATE")))
      .withColumn("month", month(col("UPDATE_DATE")))
      .withColumn("day", dayofmonth(col("UPDATE_DATE")))
      .withColumn("hour", hour(col("UPDATE_DATE")))
      .withColumn("minute", minute(col("UPDATE_DATE")))
      .drop("UPDATE_DATE")
      .repartition(1)
      .write.mode("overwrite")
      .partitionBy("year", "month", "day")
      .format("csv").save(path)
  }

  def writeCommon(dfData: DataFrame, path: String)(implicit spark: SparkSession, prop: Properties): Unit = {
    dfData
      .withColumn("year", year(col("UPDATE_DATE")))
      .withColumn("month", month(col("UPDATE_DATE")))
      .withColumn("day", dayofmonth(col("UPDATE_DATE")))
      .withColumn("hour", hour(col("UPDATE_DATE")))
      .withColumn("minute", minute(col("UPDATE_DATE")))
      .drop("UPDATE_DATE")
      .repartition(1)
      .write.mode("overwrite")
      .partitionBy("year", "month", "day")
      .format("parquet").save(path)
  }

}