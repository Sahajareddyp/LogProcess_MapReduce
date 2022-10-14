import HelperUtils.CreateLogger
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.security.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.Date
import java.util.regex.*
import scala.jdk.CollectionConverters.*

// Compute the sorted  descending order that contains most log messages of the type ERROR with injected regex pattern string instances
object MapReduce2final:
  val logger = CreateLogger(classOf[MapReduce2final.type])
  class Map extends MapReduceBase with Mapper[LongWritable, Text, IntWritable,Text]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    // swapping the key and value
    def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable,Text], reporter: Reporter): Unit =
      val line:Array[String] = value.toString.split(",") //splitting on ','
      output.collect(new IntWritable( -1* line(1).toInt), new Text(line(0)) )


  class Reduce extends MapReduceBase with Reducer[IntWritable,Text, Text, IntWritable]:
    //swapping the value and key
    override def reduce(key: IntWritable, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      //val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      //output.collect(key, new IntWritable(sum.get()))
      values.asScala.foreach(token=>
      output.collect(token, new IntWritable(-1*key.toString.toInt))
      )

  def runMapReduce2final(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    logger.info(" The sorted  descending order that contains most log messages of the type ERROR with injected regex pattern string instances: ")
    conf.setJobName("MapReduce2final")
    // conf.set("fs.defaultFS", "hdfs://localhost:9000")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapred.textoutputformat.separator", ",") //setting the separator to ','
    conf.set("mapreduce.job.reduces", "1")
    conf.setMapOutputKeyClass(classOf[IntWritable])
    conf.setMapOutputValueClass(classOf[Text])
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
   // conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/MapReduce2final"))
    JobClient.runJob(conf)
    //Starting the job for the second part of task 2



