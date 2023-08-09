package com.bluetab.example.config

import java.util.Properties

case class OracleConnect(url:String, user: String, password: String, driver: String)

object OracleConnect {

  def apply(schema: String)(implicit prop: Properties): OracleConnect = {
    schema match {
      case "shop" =>
        apply (
          prop.getProperty ("url"),
          prop.getProperty ("userShop"),
          prop.getProperty ("passwordShop"),
          prop.getProperty ("driver")
        )
    }
  }

}
