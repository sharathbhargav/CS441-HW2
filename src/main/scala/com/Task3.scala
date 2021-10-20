package com

import com.Helpers.CreateLogger
import com.Task2.logger
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

class Task3 {

}

/**
 * This task involves counting the number of messages belonging to each message type.
 */

object Task3 {
  val config = ConfigFactory.load()
  val logger = CreateLogger(classOf[Task1])
  val one = new IntWritable(1)
  class TokenMapper extends Mapper[Object, Text, Text, IntWritable] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val pattern = Pattern.compile(config.getString("mr.log_pattern"))
      val matcher = pattern.matcher(value.toString)
      if (matcher.find()) {
        val msgType = matcher.group(2)
        context.write(new Text(msgType), one)
      }
    }
  }

  class LogReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val s = values.asScala.foldLeft(0)(_ + _.get())
      context.write(key, new IntWritable(s))

    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.set("mapred.textoutputformat.separator", ",")
    val job = Job.getInstance(conf)
    job.setJarByClass(classOf[Task3])
    job.setMapperClass(classOf[TokenMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setReducerClass(classOf[LogReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    logger.info("Starting job")
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}