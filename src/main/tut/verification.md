### Demonstration of verifying data (at runtime) for Apache Dataframe

![Cats Friendly Badge](https://typelevel.org/cats/img/cats-badge-tiny.png) 

The data we are looking at looks like the following where the headers of the
CSV formatted file occupies the first line, the data lined up later.

```json
longitude,latitude,housing_median_age,total_rooms,total_bedrooms,population,households,median_income,median_house_value,ocean_proximity
-122.23,37.88,41.0,880.0,129.0,322.0,126.0,8.3252,452600.0,NEAR BAY
-122.22,37.86,21.0,7099.0,1106.0,2401.0,1138.0,8.3014,358500.0,NEAR BAY
-122.24,37.85,52.0,1467.0,190.0,496.0,177.0,7.2574,352100.0,NEAR BAY
-122.25,37.85,52.0,1274.0,235.0,558.0,219.0,5.6431,341300.0,NEAR BAY
.... # more data omitted, approximately 20k+ lines omitted
```

The code sample below is extracted from [DataVerifier.scala](../src/main/scala/examples/DataVerifier.scala)
but repeated here for your convenience.

```tut
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
```

To run it from `sbt console`, run either `run no-trace` to see a vanilla run or
`run trace` to see a traced run; once you select the appropriate example, you
should see the following :

```
...
19/08/15 18:32:45 INFO CodeGenerator: Code generated in 20.171392 ms
19/08/15 18:32:45 INFO CodeGenerator: Code generated in 8.626454 ms
19/08/15 18:32:45 INFO CodeGenerator: Code generated in 6.596286 ms
+-------+------------------+------------+------------------+
| entity|          instance|        name|             value|
+-------+------------------+------------+------------------+
| Column|     median_income|Completeness|0.6818181818181818|
| Column|housing_median_age|Completeness|               1.0|
| Column|       total_rooms|Completeness|               1.0|
| Column|median_house_value|Completeness|0.6818181818181818|
|Dataset|                 *|        Size|              66.0|
| Column|        population|Completeness|0.6818181818181818|
| Column|        households|Completeness|0.6818181818181818|
| Column|   ocean_proximity|Completeness|0.6818181818181818|
| Column|         longitude|Completeness|               1.0|
| Column|          latitude|Completeness|               1.0|
+-------+------------------+------------+------------------+
...
```

