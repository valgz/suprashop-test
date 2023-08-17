package com.bluetab.example.utils

import org.apache.spark.ml.param.DoubleArrayArrayParam
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, max, to_timestamp}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object LoadUtils {

  def readFechaCarga(dfControl: DataFrame, entidad : String): String = {
    val df = dfControl.filter(col("entidad") === entidad)
    if (df.isEmpty) "0001-01-01 00:00:00.0"
    else df
      .agg(max(col("max_fecha_registro")))
      .collect()
      .map(_.getTimestamp(0))
      .mkString("")
  }

  def readFechaEntidad(df: DataFrame): String = {
    df
      .agg(max(col("update_date")))
      .collect()
      .map(_.getTimestamp(0))
      .mkString("")
  }

  def isNuevaCarga(df: DataFrame, dfControl: DataFrame, entidad: String): Boolean = {
    val fechaCarga = readFechaCarga(dfControl, entidad)
    val fechaDf = readFechaEntidad(df)
    fechaDf > fechaCarga
  }

  def insertFechaCarga(df: DataFrame, entidad: String)(implicit spark: SparkSession): DataFrame = {

    val maxFechaRegistro = readFechaEntidad(df)

    val data = Seq(Row(entidad, maxFechaRegistro))
    val schema = StructType(
      Array(
        StructField("entidad", StringType, true),
        StructField("aux", StringType, true)
      )
    )

    spark
      .createDataFrame(spark.sparkContext.parallelize(data), schema)
      .withColumn("max_fecha_registro", to_timestamp(col("aux")))
      .drop("aux")
  }
}
