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
import org.scalatest.funsuite.AnyFunSuite
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.logging.LogFactory

import java.text.{DateFormat, SimpleDateFormat}

object test
  class test extends AnyFunSuite:

    test("Test to check if Time Parsing is done") {
      val formatter: DateFormat = new SimpleDateFormat("hh:mm:ss");
      val start = formatter.parse("22:03:00")
      assert(start.getClass.getName == "java.util.Date")
    }

    test("Test to check if configuration file is present") {
      val config: Config = ConfigFactory.load("application.conf");
      assert(!config.isEmpty())
    }

    test(" Test to check for log messsage type") {
      val line: String = "16:37:33.493 [scala-execution-context-global-15] WARN  HelperUtils.Parameters$ - g*)qMH82jo(W4S02Okh5dk`soDn3JU'e8u;?q2#s.3FarE>@:cz%"
      // Create a pattern variable for regex in log file
      val pattern = Pattern.compile("ERROR|WARN|DEBUG|INFO")
      //Create a matcher variable to match the regex given above
      val matcher = pattern.matcher(line)
      assert(matcher.find())
    }

    test("Test to check for injected pattern") {
      val line: String = "22:55:03.653 [scala-execution-context-global-15] INFO  HelperUtils.Parameters$ - d2T9S-URX8h!Q^:2ILpL,[OrS&yjjbe0W7jH6lce0cg2B7J2_0/4_Qu6!m~6u}~^X<]]%~&X"
      val pattern1 = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
      // Create another matcher variable to match the designated regex pattern
      val matcher1 = pattern1.matcher(line)
      assert(matcher1.find())
    }

    test("Test to check for injected pattern not matching") {
      val line: String = "22:55:01.825 [scala-execution-context-global-15] INFO  HelperUtils.Parameters$ - c0#hEbmBR0iETGX`^nbDLw\\uSDl>jaWpy`l1"
      val pattern1 = Pattern.compile("([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}")
      // Create another matcher variable to match the designated regex pattern
      val matcher1 = pattern1.matcher(line)
      assert(!matcher1.find())
    }
