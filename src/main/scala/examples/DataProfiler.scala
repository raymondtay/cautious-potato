package thalesdigital.io.examples

import thalesdigital.io.app.APIs
import com.amazon.deequ.checks._
import com.amazon.deequ.constraints._
import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, Completeness, Mean, Correlation, Compliance, InMemoryStateProvider, Size, DoubleValuedState}
import thalesdigital.io.deequ._
import org.apache.spark.sql._

/**
 * Run this demo to see how to run profiling leveraging deequ on good datasets.
 * There are two effects at play here:
 * (a) Obtains all statistics for all columns
 * (b) Obtains the statistic for a single column, in this case its "housing_median_age"
 *
 * @author Raymond Tay
 * @version 1.0
 */
object DataProfiler extends App with APIs {

  import cats._, cats.data._, cats.implicits._

  val defaultTracerName = "profilertracer"

  import functions._ // import ALL the spark sql functions

  Either.catchNonFatal(
    loadCsvEffectNClose(
      sys.env("TMPDIR"),
      "src/main/resources/good_data.csv",
      getStatsForAllColumns.run *> getStatsForColumn(col("housing_median_age")).run )
  )

}


