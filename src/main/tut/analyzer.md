
#### Demonstration of Analyzing Apache Dataframes

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

The code sample below is extracted from [DataAnalyzer.scala](../src/main/scala/examples/DataAnalyzer.scala)
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
```
To run it via `sbt console`, enter either `run trace-analysis` or `run trace-analysis-store-locally`
and select the appropriate program.

