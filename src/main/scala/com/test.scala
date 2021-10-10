package com

import java.lang
import java.util.regex.{Matcher, Pattern}
import java.util.StringTokenizer
class test{}
object test extends App {
    val GLOBAL_PATTERN = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
      val pattern = Pattern.compile("(.*) \\[.*\\] (INFO|WARN|DEBUG|ERROR) .* - (.*)")
      try{
      val matcher = pattern.matcher("19:26:29.877 [scala-execution-context-global-13] INFO  HelperUtils.Parameters$ - 8G;,3m_T`G#H]&Yh:Ei1%fp''5`be3O9wL9lM6oL8rbe0B9qcg1V9u%<Bqz8fMm#{JWqMdoc_2N/|wf8]")
      if(matcher.find()){
        val key = matcher.group(1)
          val msg = matcher.group(2)
      val globalMatcher = GLOBAL_PATTERN.matcher(matcher.group(3))
      println(key)
            println(msg)
      if(globalMatcher.find())
      {
            
            println(globalMatcher.group(0))
      }
      else
      println("match error")
      }
      }
      catch{
        case x: Exception =>{
          print("Caught exception "+x)
        }
      }
}