package com
import WordCount.TokenizerMapper.word
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.apache.hadoop.mapreduce.Reducer

import java.lang
import scala.collection.JavaConverters._
class WordCount {

}
object WordCount {


  import java.io.IOException
  import java.util.StringTokenizer

  object TokenizerMapper {
    private val one = new IntWritable(1)
    private val word = new Text()
  }

  class TokenizerMapper extends Mapper[Object,Text,Text,IntWritable] {
    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val itr = new StringTokenizer(value.toString)
      while ( itr.hasMoreTokens) {
        word.set(itr.nextToken)
        context.write(word, TokenizerMapper.one)
      }
    }
  }
  /*

   */

  class IntSumReducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val s = values.asScala.foldLeft(0)(_+_.get())
      context.write(key,new IntWritable(s))
    }

  }

  def main(args: Array[String]):Unit = {
    val conf = new Configuration()
    val job = Job.getInstance(conf,"word count")
    job.setJarByClass(classOf[WordCount])
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