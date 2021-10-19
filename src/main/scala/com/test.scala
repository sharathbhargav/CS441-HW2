package com

import java.lang
import java.util.regex.{Matcher, Pattern}
import java.util.StringTokenizer

class test {}

object test  {
  val GLOBAL_PATTERN = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
//  val pattern1 = Pattern.compile("(.*) [.*] ([INFO|WARN|DEBUG|ERROR]) .* - (.*)")
  val pattern1 = Pattern.compile("(.*)\\s*\\[.*\\]\\s*(INFO|WARN|DEBUG|ERROR)\\s*\\-\\s*(.*)")

  val pattern2 = Pattern.compile("(\\d+)\\s+(\\d+)")
  try {
    val s2 = "3499\t2"
    val s1 = "12:47:36.883 [scala-execution-context-global-13] INFO  - A*P^26]ie{|fspGVi}/_K0,|\\+>]\"#(<iRYzFw%<G\"7.\\ZK33>r_5zE1x2[B"
    val matcher = pattern1.matcher(s1)
    if (matcher.find()) {
      val key = matcher.group(1)
      val msg = matcher.group(2)
      val globalMatcher = GLOBAL_PATTERN.matcher(matcher.group(3))
      println(key)
      println(msg)
      println(matcher.group(3))
      if (globalMatcher.find()) {

        println(globalMatcher.group(0))
      }
      else
        println("match error")
    }
    else{
      println("Top match failed")
    }
  }
  catch {
    case x: Exception => {
      print("Caught exception " + x)
    }
  }
}