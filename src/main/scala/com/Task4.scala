package com

import com.Helpers.CreateLogger

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import java.lang
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.IterableHasAsScala

class Task4 {
}

object Task4 {
  val config = ConfigFactory.load()
  val logger = CreateLogger(classOf[Task4])
  class TokenMapper1 extends Mapper[Object, Text, Text, Text] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
      val pattern = Pattern.compile(config.getString("mr.log_pattern"))
      val GLOBAL_PATTERN = Pattern.compile(config.getString("mr.detect_pattern"))
      val matcher = pattern.matcher(value.toString)
      if (matcher.find()) {
        val key = matcher.group(2)
        val msg = matcher.group(3)
        val global_matcher = GLOBAL_PATTERN.matcher(msg.toString)
        if (global_matcher.find()) {
          val matched = global_matcher.group(0)
          context.write(new Text(key), new Text(matched))
        }
      }
    }
  }

  class LogReducer1 extends Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
      val k = values.asScala.max
      context.write(new Text(key.toString), new Text(k.getLength.toString))
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val job = Job.getInstance(conf)
    conf.set("mapred.textoutputformat.separator", ",")
    job.setJarByClass(classOf[Task4])
    job.setMapperClass(classOf[TokenMapper1])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setReducerClass(classOf[LogReducer1])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}