package com

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

class Task1 {

}
object Task1 {
  class TokenMapper extends Mapper[Object,Text,Text,IntWritable ]{
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val pattern = Pattern.compile("(.*)\\s*\\[.*\\]\\s*(INFO|WARN|DEBUG|ERROR)\\s*\\-\\s*(.*)")
      val matcher = pattern.matcher(value.toString)
      val interval = context.getConfiguration.get("interval").toInt
      if(matcher.find()){
        val GLOBAL_PATTERN = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
        val dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS")
        val date = (dateFormatter.parse(matcher.group(1).toString).getTime)/(1000)
        val d1 = (date.toInt)/interval
        val key = matcher.group(2)
        val msg = matcher.group(3)
        val global_matcher = GLOBAL_PATTERN.matcher(msg.toString)
        val nM = key +","+ d1.toString
        if(global_matcher.find())
          context.write(new Text(nM),new IntWritable(1))
      }
    }
  }
  class LogReducer extends Reducer[Text,IntWritable,Text,IntWritable]{
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val s = values.asScala.foldLeft(0)(_+_.get())
      val str = key.toString.split(",")
      val strDate = UtilityFunc.convertTimeStampToString(str(1).toString,context.getConfiguration.get("interval").toInt)
      context.write(new Text(strDate+ " "+str(0)),new IntWritable(s))

    }
  }

  def main(args:Array[String]):Unit = {
    val conf=new Configuration()
    conf.set("interval",args(2))
    val job = Job.getInstance(conf)
    job.setJarByClass(classOf[Task1])
    job.setMapperClass(classOf[TokenMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setReducerClass(classOf[LogReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job,new Path(args(0)))
    FileOutputFormat.setOutputPath(job,new Path(args(1)))
    System.exit(if(job.waitForCompletion(true)) 0 else 1)
  }
}
