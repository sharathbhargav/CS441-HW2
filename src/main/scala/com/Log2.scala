package com

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.join.TupleWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import java.text.SimpleDateFormat
import java.util.regex.{Matcher, Pattern}
import java.util.{Date, Locale, StringTokenizer}
import scala.jdk.CollectionConverters.IterableHasAsScala
class Log2 {

}
object Log2 {
  class TokenizerMapper extends Mapper[Object,Text,Text,IntWritable] {
    val one  = new IntWritable(1)
    val GLOBAL_PATTERN = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val word = new Text()
      val conf = context.getConfiguration
      val param = conf.get("interval").toInt
      val pattern = Pattern.compile("(.*) \\[.*\\] (INFO|WARN|DEBUG|ERROR) .* - (.*)")
      try{
        val matcher = pattern.matcher(value.toString)
        if(matcher.find()){
          val key = matcher.group(2)
          val msg = matcher.group(3).toString
          val formatter = new SimpleDateFormat("HH:mm:ss.SSS", Locale.ENGLISH)
          val date = formatter.parse(matcher.group(1).toString)
          val interval = ((date.getTime / 1000) / param).toInt
          val globalMatcher = GLOBAL_PATTERN.matcher(msg)
          word.set(key.toString+","+interval.toString)
            context.write(word, one)
          
        }
      }
      catch{
        case x: Exception =>{
          print("Caught exception ")
        }
      }
    }
  }
  class IntSumReducer extends Reducer[Text,IntWritable,Text,Text] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
      val s = values.asScala.foldLeft(0)(_+_.get())
      val msg = key.toString.split(",")
      val msgType =new Text()
      msgType.set(msg(0).toString())
      context.write(msgType,new Text(msg(1).toString()+" "+s.toString))
    }

  }
  def main(args: Array[String]):Unit = {
    val conf = new Configuration()
    conf.set("interval",args(2))
    val job = Job.getInstance(conf,"log count")
    job.setJarByClass(classOf[Log1])
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job,new Path(args(1)))
    println("Added job");
    System.exit(  if (job.waitForCompletion(true)) 0 else 1)
  }
}