package com.Helpers

import java.text.SimpleDateFormat
import java.util.Date

class UtilityFunc {

}

object UtilityFunc{
  def convertTimeStampToString( str:String,interval:Int):String={
    val d = new Date(str.toInt*(interval)*1000)
    val dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS")
    return dateFormatter.format(d)
  }
}