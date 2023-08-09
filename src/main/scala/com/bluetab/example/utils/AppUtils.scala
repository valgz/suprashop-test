package com.bluetab.example.utils

import java.util.Properties

object AppUtils {

  def loadPropertiesFile(): Properties = {
    val properties = new Properties()
    properties.load(this.getClass.getResourceAsStream("/properties/application.properties"))
    properties
  }

}
