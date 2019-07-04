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
object DataVerifier extends App with APIs {

  import cats._, cats.data._, cats.implicits._
  import functions._ // imports the sugarcoated functions for spark sql

  val defaultTracerName = "data-verifier-tracer"

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

  // Defined a function that carries 2 effects:
  // (a) load the CSV into memory and run the data against "checks"
  // (b) print the outcome of the data verification
  def traceItNo =
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

  // Defined a function that carries 3 effects:
  // (a) load the CSV into memory and run the data against "checks"
  // (a.1) after the checks are done, the result is sent as a traced to
  // OpenTracing logger
  // (b) print the outcome of the data verification
  def traceItYes =
      Either.catchNonFatal{
          traceLoadCsvEffectNClose(
            sys.env("TMPDIR"),
            "src/main/resources/bad_data.csv",
            runDataWithChecks(check).run andThen sendVerificationResultToLogstore.run
          )
      }

  // Run it !
  if (!args.headOption.isEmpty && args.headOption.get.equalsIgnoreCase("no-trace"))
    traceItNo
  else if (!args.headOption.isEmpty && args.headOption.get.equalsIgnoreCase("trace"))
    traceItYes
  else {
    println("You did not indicate an valid option. Exiting...")
    System.exit(-1)
  }

}

