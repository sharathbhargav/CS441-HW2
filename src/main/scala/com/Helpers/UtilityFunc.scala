package com.Helpers

import java.text.SimpleDateFormat
import java.util.Date

class UtilityFunc {

}

object UtilityFunc{

  /**
   * This function takes in interval bin number in string format, then converts the  and returns a readable representation of time. Effectively reversing the actions done in mapper
   * @param str
   * @param interval
   * @return
   */
  def convertTimeStampToString( str:String,interval:Int):String={
    val d = new Date(str.toInt*(interval)*1000)
    val dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS")
    return dateFormatter.format(d)
  }
}