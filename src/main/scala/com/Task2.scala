package com

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.lang
import java.text.SimpleDateFormat
import java.util.{Date, StringTokenizer}
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.IterableHasAsScala

class Task2 {

}

object Task2 {

  class Map1 extends Mapper[Object, Text, IntWritable, IntWritable] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {
      val pattern = Pattern.compile("(.*)\\s*\\[.*\\]\\s*(ERROR)\\s*\\-\\s*(.*)")
      val matcher = pattern.matcher(value.toString)
      val interval = context.getConfiguration.get("interval").toInt
      if (matcher.find()) {
        val GLOBAL_PATTERN = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
        val dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS")
        val date = (dateFormatter.parse(matcher.group(1).toString).getTime) / (1000)
        val d1 = (date.toInt) / interval

        val key = matcher.group(2)
        val msg = matcher.group(3)
        val global_matcher = GLOBAL_PATTERN.matcher(msg.toString)
        val nM = d1
        if (global_matcher.find())
          context.write(new IntWritable(nM), new IntWritable(1))
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

  class DescendingIntComparator() extends WritableComparator(classOf[IntWritable], true) {
    @SuppressWarnings(Array("rawtypes"))
    override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
      val key1 = w1.asInstanceOf[IntWritable]
      val key2 = w2.asInstanceOf[IntWritable]
      -1 * key1.compareTo(key2)
    }
  }

  class Reduce2 extends Reducer[IntWritable, Text, Text, Text] {
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
    if (job.waitForCompletion(true)) {
      val job1 = Job.getInstance(conf, "Job2")
      job1.setJarByClass(classOf[Task1])

      job1.setMapperClass(classOf[Map2])
      job1.setMapOutputKeyClass(classOf[IntWritable])
      job1.setMapOutputValueClass(classOf[Text])
      job1.setSortComparatorClass(classOf[DescendingIntComparator])
      job1.setReducerClass(classOf[Reduce2])
      job1.setOutputKeyClass(classOf[Text])
      job1.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(job1, new Path(args(1)))
      FileOutputFormat.setOutputPath(job1, new Path(args(2)))
      System.exit(if (job1.waitForCompletion(true)) 0 else 1)
    }
  }
}