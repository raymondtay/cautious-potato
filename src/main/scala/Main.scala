package thalesdigital.io.app


import com.amazon.deequ.checks._
import com.amazon.deequ.constraints._
import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, Completeness, Mean, Correlation, Compliance, InMemoryStateProvider, Size, DoubleValuedState}
import thalesdigital.io.deequ._
import org.apache.spark.sql._

object Main extends App with APIs {

  import cats._, cats.data._, cats.implicits._

  val defaultTracerName = args(0)

  /* Uncomment the following iff you just want to load the csv, resources
   * released implicitly.
   **/
  // Either.catchNonFatal(
  //   runApiNClose(sys.env("TMPDIR"), "src/main/resources/housing.csv")
  // )

  import functions._
  //Either.catchNonFatal(
  //  loadCsvEffectNClose(
  //    sys.env("TMPDIR"),
  //    "src/main/resources/good_data.csv",
  //    getStatsForAllColumns.run *> getStatsForColumn(col("housing_median_age")).run )
  //)

  // A check that represents what the developer expects the data to exhibit.
  val check =
    Check(CheckLevel.Error, "unit testing my data")
      .hasSize(_ == 50)
      .isComplete("longitude")
      .isComplete("latitude")
      .isComplete("housing_median_age")
      .isComplete("total_rooms")
      .isComplete("total_bedrooms")
      .isComplete("population")
      .isComplete("households")
      .isComplete("median_income")
      .isComplete("median_house_value")
      .isComplete("ocean_proximity")

  // 1. Non-trace
  // Either.catchNonFatal{
  //   val result = 
  //     loadCsvEffectNClose(
  //       sys.env("TMPDIR"),
  //       "src/main/resources/good_data.csv",
  //       runDataWithChecks(check).run
  //     )
  //   println(s"""
  //    Result of verification: ${result.status}
  //   """)
  // }

  // 2. Traced execution with running data-validation
  //    and sending the final results to OpenTracing logger
  Either.catchNonFatal{
      traceLoadCsvEffectNClose(
        sys.env("TMPDIR"),
        "src/main/resources/bad_data.csv",
        runDataWithChecks(check).run andThen sendVerificationResultToLogstore.run
      )
  }

  // 3. Traced execution together with running data-analysis
  //    and sending the final results to OpenTracing logger
  val analyzer : Analysis =
    buildAnalyzers(Size(),
      ApproxCountDistinct("housing_median_age"),
      Completeness("housing_median_age"),
      Completeness("longitude"),
      Completeness("latitude")).run(Analysis())

  //Either.catchNonFatal{
  //  val result = 
  //    traceLoadCsvEffectNClose(
  //      sys.env("TMPDIR"),
  //      "src/main/resources/good_data.csv",
  //      runDataWithAnalyzers(analyzer).run
  //    )
  //}

  // 4. Non-traced execution of conducting checks and storing the metrics to
  //    local file storage
  //Either.catchNonFatal{
  //  val result = 
  //    traceLoadCsvEffectNClose(
  //      sys.env("TMPDIR"),
  //      "src/main/resources/bad_data.csv",
  //      runDataWithChecksNStorage("metrics.json", Map("tag" -> "repositoryExample"), check).run
  //    )
  //}

 Thread.sleep(5000)
 
}

