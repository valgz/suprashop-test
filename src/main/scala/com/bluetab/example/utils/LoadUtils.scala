package com.bluetab.example.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.max

object LoadUtils {

  def readFechaCarga(dfControl: DataFrame, entidad : String): String = {
    val df = dfControl.filter(col("entidad") === entidad)
    if (df.isEmpty) "0001-01-01 00:00:00"
    else df
      .agg(max(col("max_fecha_registro")))
      .collect()
      .map(_.getTimestamp(0))
      .mkString("")
  }
}
