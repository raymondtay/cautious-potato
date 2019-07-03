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
object DataVerifierRuntime extends App with APIs {

  import cats._, cats.data._, cats.implicits._
  import functions._ // imports the sugarcoated functions for spark sql

  val defaultTracerName = "data-verifier-no-trace"

  // A check that represents what the developer expects the data to exhibit.
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

  Either.catchNonFatal{
    val result = 
      loadCsvEffectNClose(
        sys.env("TMPDIR"),
        "src/main/resources/good_data.csv",
        runDataWithChecks(check).run
      )
    println(s"""
     Result of verification: ${result.status}
    """)
  }

}

