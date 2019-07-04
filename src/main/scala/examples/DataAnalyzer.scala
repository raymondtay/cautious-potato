package thalesdigital.io.examples

import thalesdigital.io.app.APIs

import com.amazon.deequ.checks._
import com.amazon.deequ.constraints._
import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, Completeness, Mean, Correlation, Compliance, InMemoryStateProvider, Size, DoubleValuedState}
import thalesdigital.io.deequ._
import org.apache.spark.sql._

/**
 * Unit-test the data by defining checks on the data columns, of course there
 * are a lot more checks you can implement but this is to start you off; you
 * can read through the source code to understand more.
 *
 * Over here, we assert that no data columns are null or empty and this dataset
 * should be of size 20,640 rows.
 *
 * @author Raymond Tay
 * @version 1.0
 */
object DataAnalyzer extends App with APIs {

  import cats._, cats.data._, cats.implicits._
  import functions._ // imports the sugarcoated functions for spark sql

  val defaultTracerName = "data-analyzer-tracer"

  // A [[Check]] in Deequ parlance can be either: (a) constraint property (b) consistency property (b) statistical property
  // which is defined by data engineers, typically, which performs checks
  // during runtime against the data (typically, against the runtime)
  //
  val check =
    Check(CheckLevel.Error, "unit testing my data")
      .hasSize(_ == 20640)
      .isComplete("longitude")
      .isComplete("latitude")
      .isComplete("housing_median_age")
      .isComplete("total_rooms")
      //.isComplete("total_bedrooms") // This supposedly "good dataset" is actually problematic
      .isComplete("population")
      .isComplete("households")
      .isComplete("median_income")
      .isComplete("median_house_value")
      .isComplete("ocean_proximity")

  // A [[Analyzer]] in Deequ parlance is a runtime activity that runs the
  // analysis desired against the data.
  //
  val analyzer : Analysis =
    buildAnalyzers(Size(),
      ApproxCountDistinct("housing_median_age"),
      Completeness("housing_median_age"),
      Completeness("longitude"),
      Completeness("latitude")).run(Analysis())

  def traceDataAnalysis =
      Either.catchNonFatal{
        val result = 
          traceLoadCsvEffectNClose(
            sys.env("TMPDIR"),
            "src/main/resources/good_data.csv",
            runDataWithAnalyzers(analyzer).run
          )
      }

  def traceDataAnalysisWithLocalStorage =
      Either.catchNonFatal{
        val result = 
          traceLoadCsvEffectNClose(
            sys.env("TMPDIR"),
            "src/main/resources/bad_data.csv",
            runDataWithChecksNStorage("metrics.json", Map("tag" -> "repositoryExample"), check).run
          )
      }

  if (!args.headOption.isEmpty && args.headOption.get.equalsIgnoreCase("trace-analysis"))
    traceDataAnalysis
  else if (!args.headOption.isEmpty && args.headOption.get.equalsIgnoreCase("trace-analysis-store-locally"))
          traceDataAnalysisWithLocalStorage
        else {
          println("You entered no option. Exiting.")
          System.exit(-1)
        }
}

