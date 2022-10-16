import HelperUtils.CreateLogger
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import java.util.regex.*
import scala.jdk.CollectionConverters.*
import com.typesafe.config.{Config, ConfigFactory}

//computes the number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern.
object MapReduce4:
  val logger = CreateLogger(classOf[MapReduce4.type])
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    // Import the Configuration from application.conf
    val conf: Config = ConfigFactory.load("application.conf");

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      // Create a pattern variable for regex in log file
      val pattern = Pattern.compile(conf.getString("randomLogGenerator.MapReduce4MsgTyp"))
      //Create a matcher variable to match the regex given
      val matcher= pattern.matcher(line)
      // Create another variable to detect the instances of the designated regex pattern given in application.conf file in the particular log file
     // val pattern1 = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
      val pattern1 = Pattern.compile(conf.getString("randomLogGenerator.Pattern"))
      // Create another matcher variable to match the designated regex pattern
      val matcher1 = pattern1.matcher(line)
      // if both the patterns are found in the log file
      if(matcher.find() && matcher1.find())
      {
        word.set(matcher.group()) //group the matched instances
        output.collect(word,new IntWritable(matcher1.group().length)) // collect the output of the matched instances
      }


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
     // to find the maximum value for collection of values
      val sum= values.asScala.max
     // write key, result as value
      output.collect(key,  new IntWritable(sum.get()))

  def runMapReduce4(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("WordCount")
    // conf.set("fs.defaultFS", "hdfs://localhost:9000")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.set("mapred.textoutputformat.separator", ",")
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/MapReduce4"))
    JobClient.runJob(conf)



