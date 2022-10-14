import HelperUtils.CreateLogger
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import java.util.regex.*
import scala.jdk.CollectionConverters.*

// produce the number of the generated log messages for each message type
object MapReduce3:
  val logger = CreateLogger(classOf[MapReduce3.type])
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    val conf: Config = ConfigFactory.load("application.conf");
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      // Create a pattern variable for regex in log file
      val pattern = Pattern.compile(conf.getString("randomLogGenerator.MapReduce3MsgTyp"))
      //Create a matcher variable to match the regex given above
      val matcher= pattern.matcher(line)
        if(matcher.find())
        {
          word.set(matcher.group()) //group the matched instances
          output.collect(word, one) // collect the output of the matched instances
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      // sum of the generated log messages for each message type
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))

  def runMapReduce3(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("WordCount")
    // conf.set("fs.defaultFS", "hdfs://localhost:9000")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.set("mapred.textoutputformat.separator", ",")
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/MapReduce3"))
    JobClient.runJob(conf)

