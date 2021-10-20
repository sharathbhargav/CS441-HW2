package com

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import com.Helpers.{CreateLogger, UtilityFunc}
import com.typesafe.config.ConfigFactory

import java.lang
import java.text.SimpleDateFormat
import java.util.{Date, StringTokenizer}
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.IterableHasAsScala

class Task2 {

}
/**
 * This map reduce job takes in log files in a folder as input and outputs the number of error messages
 * divided across time intervals of n seconds where n is passed as a parameter while
 * running the program. The output is sorted in descending order of number of messages.
 * This task involves 2 map reduce jobs. The first job detects that count of error messages that match the pattern.
 * The second task sorts the output in descending order by using a custom comparator function that is called before
 * reduce stage.
 *
 */
object Task2 {
  val config = ConfigFactory.load()
  val logger = CreateLogger(classOf[Task2])
  val one = new IntWritable(1)

  class Map1 extends Mapper[Object, Text, IntWritable, IntWritable] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {
      val pattern = Pattern.compile(config.getString("mr.log_pattern"))
      val matcher = pattern.matcher(value.toString)
      val interval = context.getConfiguration.get("interval").toInt
      if (matcher.find()) {
        val GLOBAL_PATTERN = Pattern.compile(config.getString("mr.detect_pattern"))
        val dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS")
        val date = (dateFormatter.parse(matcher.group(1).toString).getTime) / (1000) //Divide the epoch time by 1000 to convert from milli seconds to seconds
        val d1 = (date.toInt) / interval
        val msg = matcher.group(3)
        val global_matcher = GLOBAL_PATTERN.matcher(msg.toString)
        if (global_matcher.find())
          context.write(new IntWritable(d1), one)
      }
    }
  }

  class Reduce1 extends Reducer[IntWritable, IntWritable, Text, IntWritable] {
    override def reduce(key: IntWritable, values: lang.Iterable[IntWritable], context: Reducer[IntWritable, IntWritable, Text, IntWritable]#Context): Unit = {
      val s = values.asScala.foldLeft(0)(_ + _.get())
      context.write(new Text(key.get().toString), new IntWritable(s))
    }
  }

  class Map2 extends Mapper[Object, Text, IntWritable, Text] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val pattern = Pattern.compile("(\\d+)\\s+(\\d+)")
      val matcher = pattern.matcher(value.toString)
      if (matcher.find())
        context.write(new IntWritable(matcher.group(2).toInt), new Text(matcher.group(1)))
    }
  }

  /**
   * Custom comparator class to sort the output of mapper in descending order.
   */
  class DescendingIntComparator() extends WritableComparator(classOf[IntWritable], true) {
    @SuppressWarnings(Array("rawtypes"))
    override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
      val key1 = w1.asInstanceOf[IntWritable]
      val key2 = w2.asInstanceOf[IntWritable]
      -1 * key1.compareTo(key2)
    }
  }

  class Reduce2 extends Reducer[IntWritable, Text, Text, Text] {
    // Optional step to add headers to csv file. Have to change the datatype of output in reducer as well to text.
    override def setup(context: Reducer[IntWritable, Text, Text, Text]#Context): Unit = {
      context.write(new Text("Number of messages"),new Text("Interval start time"))
    }

    override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, Text]#Context): Unit = {

      values.forEach(each => {
        val strDate = UtilityFunc.convertTimeStampToString(each.toString, context.getConfiguration.get("interval").toInt)
        context.write(new Text(key.get().toString), new Text(strDate))
      })
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.set("interval", args(3))
    val job = Job.getInstance(conf, "Job1")
    job.setJarByClass(classOf[Task2])
    job.setMapperClass(classOf[Map1])
    job.setReducerClass(classOf[Reduce1])
    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    logger.info("Starting map reduce job 1 for task1. This job will find log messages that match the pattern defined in config file and returns their count across the " +
      "interval defined by the 4th parameter")
    if (job.waitForCompletion(true)) {
      val job1 = Job.getInstance(conf, "Job2")
      job1.setJarByClass(classOf[Task2])
      conf.set("mapred.textoutputformat.separator", ",")
      job1.setMapperClass(classOf[Map2])
      job1.setMapOutputKeyClass(classOf[IntWritable])
      job1.setMapOutputValueClass(classOf[Text])
      job1.setSortComparatorClass(classOf[DescendingIntComparator])
      job1.setReducerClass(classOf[Reduce2])
      job1.setNumReduceTasks(1)
      job1.setOutputKeyClass(classOf[Text])
      job1.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(job1, new Path(args(1)))
      FileOutputFormat.setOutputPath(job1, new Path(args(2)))
      logger.info("Starting map reduce job 2 for task2. This job will essentially only sort the output of job 1 in descending order of number of messages." +
        "Here only 1 reducer is defined so that all results can be aggregated in one output file. This step is optional")
      System.exit(if (job1.waitForCompletion(true)) 0 else 1)
    }
  }
}