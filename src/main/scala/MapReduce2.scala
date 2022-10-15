import HelperUtils.CreateLogger
import com.typesafe.config.{Config, ConfigFactory}
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

//Computing the most log messages of the type ERROR with injected regex pattern string instances
object MapReduce2:
  val logger = CreateLogger(classOf[MapReduce2.type])
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    val conf: Config = ConfigFactory.load("application.conf");

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      // Create a pattern variable for regex type ERROR
          val pattern = Pattern.compile(conf.getString("randomLogGenerator.MapReduce2MsgTyp"))
      //Create a matcher variable to match the regex given above
          val matcher = pattern.matcher(line)
      // Create another variable to detect the instances of the designated regex pattern given in application.conf file in the particular log file
          //val pattern1 = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
        val pattern1 = Pattern.compile(conf.getString("randomLogGenerator.Pattern"))
      // Create another matcher variable to match the designated regex pattern
          val matcher1 = pattern1.matcher(line)
      // if both the patterns are found in the log file
          if (matcher.find() && matcher1.find())
          {
            word.set(line.substring(0,5)+":00 "+matcher.group()) //choosing the time format to a particular digit count
            output.collect(word, one)
          }
          else
            logger.error("No ERROR log entries in the specified time interval")

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      //sum of different types of messages across predefined time intervals
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))

  def runMapReduce2(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("MapReduce2")
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
    FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/mapreduce2"))
    JobClient.runJob(conf)
    MapReduce2final.runMapReduce2final(outputPath + "/mapreduce2/part-00000",outputPath)


