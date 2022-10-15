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

 // compute a  CSV file that shows the distribution of different types of messages across predefined time intervals and injected string instances of the designated regex pattern for these log message types. 
object MapReduce1:
  val logger = CreateLogger(classOf[MapReduce1.type])
  //Loading configuration files from application.conf
  val conf: Config = ConfigFactory.load("application.conf");
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()
    val conf: Config = ConfigFactory.load("application.conf");

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      // defining time as String and selecting the time format
      val time1 =line.substring(0,9)
      logger.info("time is "+ time1)

      logger.info("Defining a particular time interval to show the distribution of different types of messages ")
      // taking the start time from application.conf
      val starttime= conf.getString("randomLogGenerator.starttime")//"22:02:00"
      // taking the end time from application.conf
      val endtime=conf.getString("randomLogGenerator.endtime")//"22:07:00"
      // Defining time as String and parsing it as Date format
      val formatter : DateFormat = new SimpleDateFormat("hh:mm:ss");
      // checking if string starts with time format
      if(Pattern.compile("^(\\d\\d:\\d\\d:\\d\\d)").matcher(value.toString.substring(0,8)).find())
        {
          val date: Date = formatter.parse(time1);
          //val timeStampDate: Timestamp  = new Timestamp(date.getTime())
          //parsing String into Date
          val start: Date = formatter.parse(starttime)
          val end: Date = formatter.parse(endtime)
          logger.info("The computed CSV file that shows the distribution of different types of messages across predefined time intervals")
          if (date.after(start) && date.before(end)) {
            // Compiling for regex pattern
            val pattern = Pattern.compile(conf.getString("randomLogGenerator.MapReduce1MsgTyp"))
            val matcher = pattern.matcher(line)
            logger.info("Matching the string instances of the designated regex pattern for the above log message types")
            val pattern1 = Pattern.compile(conf.getString("randomLogGenerator.Pattern"))
            // Matching the regex pattern to the pattern given in application.conf
            val matcher1 = pattern1.matcher(line)
            if (matcher.find() && matcher1.find()) {
              // to find the
              word.set(time1.substring(0, 5) + ":00 " + matcher.group()) //choosing the time format to a particular digit count
              output.collect(word, one)
            }
          }
        }


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("Starting the Reducer for Job 1")
      //sum of different types of messages across predefined time intervals
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))

  def runMapReduce1(inputPath: String, outputPath: String) =
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
    FileOutputFormat.setOutputPath(conf, new Path(outputPath + "/MapReduce1"))
    JobClient.runJob(conf)



