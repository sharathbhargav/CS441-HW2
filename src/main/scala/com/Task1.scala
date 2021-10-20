package com

import com.Helpers.{CreateLogger, UtilityFunc}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * This map reduce job takes in log files in a folder as input and outputs the number of messages of each log type
 * (INFO, DEBUG, WARN, ERROR) divided across time intervals of n seconds where n is passed as a parameter while
 * running the program.
 *
 */

class Task1 {

}

object Task1 {
  val config = ConfigFactory.load()
  val logger = CreateLogger(classOf[Task1])
  val one = new IntWritable(1)
  class TokenMapper extends Mapper[Object, Text, Text, IntWritable] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val pattern = Pattern.compile(config.getString("mr.log_pattern"))
      val matcher = pattern.matcher(value.toString)
      val interval = context.getConfiguration.get("interval").toInt
      if (matcher.find()) {
        val GLOBAL_PATTERN = Pattern.compile(config.getString("mr.detect_pattern"))
        val dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS")
        val date = (dateFormatter.parse(matcher.group(1)).getTime) / (1000) //Divide the epoch time by 1000 to convert from milli seconds to seconds
        val d1 = (date.toInt) / interval // Divide th
        val key = matcher.group(2)
        val msg = matcher.group(3)
        val global_matcher = GLOBAL_PATTERN.matcher(msg)
        val nM = key + "," + d1.toString // Passing a pseudo composite key to represent the tuple of time interval and msg type
        if (global_matcher.find())
          context.write(new Text(nM),one)
      }
    }
  }

  class LogReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val s = values.asScala.foldLeft(0)(_ + _.get())
      val str = key.toString.split(",")
      val strDate = UtilityFunc.convertTimeStampToString(str(1).toString, context.getConfiguration.get("interval").toInt)
      context.write(new Text(strDate + "," + str(0)), new IntWritable(s))

    }
  }


  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.set("interval", args(2))
    conf.set("mapred.textoutputformat.separator", ",")
    val job = Job.getInstance(conf)
    job.setJarByClass(classOf[Task1])
    job.setMapperClass(classOf[TokenMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setReducerClass(classOf[LogReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    logger.info("Starting map reduce task 1")
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }

}
