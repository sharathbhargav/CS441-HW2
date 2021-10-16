package com

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import java.util.regex.{Matcher, Pattern}
import java.util.StringTokenizer
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * Solution to Part 3 of home work where we have to calculate the total number of logs for each type of message
 */

class Log1 {
}
object Log1 {
  class TokenizerMapper extends Mapper[Object,Text,Text,IntWritable] {
    val one  = new IntWritable(1)
    val GLOBAL_PATTERN = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val word = new Text()
      val pattern = Pattern.compile("(INFO|WARN|DEBUG|ERROR) .* - (.*)")
      val matcher = pattern.matcher(value.toString)
      if(matcher.find()){
        val key = matcher.group(1)
        val msg = matcher.group(2).toString
        word.set(key.toString)
        context.write(word, one)
        
      }
      
    }
  }
  class IntSumReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val s = values.asScala.foldLeft(0)(_+_.get())
      context.write(key,new IntWritable(s))
    }

  }
  def main(args: Array[String]):Unit = {
    val conf = new Configuration()
    val job = Job.getInstance(conf,"log count")
    job.setJarByClass(classOf[Log1])
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job,new Path(args(1)))
    println("Added job");
    System.exit(  if (job.waitForCompletion(true)) 0 else 1)
  }
}