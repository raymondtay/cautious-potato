package thalesdigital.io.examples

import thalesdigital.io.app.APIs

import thalesdigital.io.deequ._
import org.apache.spark.sql._

import com.amazon.deequ.analyzers._
import com.amazon.deequ.anomalydetection.RateOfChangeStrategy
import com.amazon.deequ.checks._
import com.amazon.deequ.checks.CheckStatus._
import com.amazon.deequ.constraints._
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository

/**
 * This example leverages two (2) CSV files i.e. bad_data.csv and bad_data2.csv
 * for this and there are 2 computations being done. First is to compute the
 * statistics for one dataset and compute the statistics for the second dataset
 * and finally deequ is leveraged to compute the final result.
 *
 * Given, the constraints we placed onto the detection we should be able to
 * detect it and view it on the OpenTracing logger.
 *
 * @author Raymond Tay
 * @version 1.0
 */
object DataAnomalyDetector extends App with APIs {

  import cats._, cats.data._, cats.implicits._
  import functions._ // imports the sugarcoated functions for spark sql

  val defaultTracerName = "data-anomaly-tracer"
  
  val yesterday = System.currentTimeMillis() - 24 * 60 * 1000
  val today = System.currentTimeMillis()

  def traceDataAnomalies = {
    Either.catchNonFatal{
      val (verificationResult1, repo) = 
        traceLoadCsvEffectNClose(
          sys.env("TMPDIR"),
          "src/main/resources/bad_data.csv", // data here is bad....
          buildAnomalyDetection(none,
                                yesterday, // represents yesterday
                                Map(),
                                RateOfChangeStrategy(maxRateIncrease = Some(2.0)),
                                Size().some).run)
      val (verificationResult2, repo2) = 
        traceLoadCsvEffectNClose(
          sys.env("TMPDIR"),
          "src/main/resources/bad_data2.csv", // data here is worse than bad
          buildAnomalyDetection(repo.some,
                                today, // represents today
                                Map(),
                                RateOfChangeStrategy(maxRateIncrease = Some(2.0)),
                                Size().some).run)

      if (verificationResult2.status != Success) {
        println("Anomaly detected in the Size() metric!")
        getSparkSession(sys.env("TMPDIR")) >>=
          {(session: SparkSession) => 
              repo2.load().forAnalyzers(Seq(Size())).getSuccessMetricsAsDataFrame(session).show()
          }
      }
      else println("we are done.")
 
    }
  }

  if (!args.headOption.isEmpty && args.headOption.get.equalsIgnoreCase("trace-anomaly"))
    traceDataAnomalies
  else {
    println("You entered no option. Exiting.")
    System.exit(-1)
  }

}

