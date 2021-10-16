package com

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.lang
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.IterableHasAsScala

class Task4 {

}


object Task4 {
  class TokenMapper extends Mapper[Object, Text, Text, Text] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
      val pattern = Pattern.compile("(.*)\\s*\\[.*\\]\\s*(INFO|WARN|DEBUG|ERROR)\\s*\\-\\s*(.*)")

      val GLOBAL_PATTERN = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")

      val matcher = pattern.matcher(value.toString)
      if (matcher.find()) {
        val key = matcher.group(2)
        val msg = matcher.group(3)
        val global_matcher = GLOBAL_PATTERN.matcher(msg.toString)
        if (global_matcher.find()) {
          val matched = global_matcher.group(0).toString
          val matchedLen = matched.length
          context.write(new Text(key), new Text(matched))

        }

      }
    }
  }

  class LogReducer extends Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      val s = values.asScala.foldLeft("")(_ + "\n "+_.toString)

      val iter = values.iterator()
      var m=0
      var ms=""
      while(iter.hasNext){
        val m1= iter.next()
        if(m1.toString.length>m){
          m=m1.toString.length
          ms=m1.toString
          context.write(new Text(key.toString), new Text(ms+","+m.toString))

        }
      }
      context.write(new Text(key.toString), new Text(ms+","+m.toString+" "+s))

    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val job = Job.getInstance(conf)
    job.setJarByClass(classOf[Task4])
    job.setMapperClass(classOf[TokenMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setReducerClass(classOf[LogReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}