package thalesdigital.io.app


import com.amazon.deequ.checks._
import com.amazon.deequ.constraints._
import thalesdigital.io.datachecker.DeequTools
import org.apache.spark.sql._

object Main extends App with APIs with DeequTools {

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

  // This check is guaranteed to be successful.
  val check =
    Check(CheckLevel.Error, "unit testing my data")
      .hasSize(_ == 20640)
      .isComplete("housing_median_age")

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

  // 2. Traced execution
  Either.catchNonFatal{
    val result = 
      traceLoadCsvEffectNClose(
        sys.env("TMPDIR"),
        "src/main/resources/good_data.csv",
        runDataWithChecks(check).run
      )
    println(s"""
     Result of verification: ${result.status}
    """)
  }

 Thread.sleep(5000)
 
}

