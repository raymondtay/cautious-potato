
#### Demonstration of Anomaly-Detection of Apache Dataframes

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

The code sample below is extracted from [DataAnomalyDetector.scala](../src/main/scala/examples/DataAnomalyDetector.scala)
but repeated here for your convenience.

```scala
scala> import thalesdigital.io.app.APIs
import thalesdigital.io.app.APIs

scala> import thalesdigital.io.deequ._
import thalesdigital.io.deequ._

scala> import org.apache.spark.sql._
import org.apache.spark.sql._

scala> import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers._

scala> import com.amazon.deequ.anomalydetection.RateOfChangeStrategy
import com.amazon.deequ.anomalydetection.RateOfChangeStrategy

scala> import com.amazon.deequ.checks._
import com.amazon.deequ.checks._

scala> import com.amazon.deequ.checks.CheckStatus._
import com.amazon.deequ.checks.CheckStatus._

scala> import com.amazon.deequ.constraints._
import com.amazon.deequ.constraints._

scala> import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.ResultKey

scala> import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository

scala> /**
     |  * This example leverages two (2) CSV files i.e. bad_data.csv and bad_data2.csv
     |  * for this and there are 2 computations being done. First is to compute the
     |  * statistics for one dataset and compute the statistics for the second dataset
     |  * and finally deequ is leveraged to compute the final result.
     |  *
     |  * Given, the constraints we placed onto the detection we should be able to
     |  * detect it and view it on the OpenTracing logger.
     |  *
     |  * @author Raymond Tay
     |  * @version 1.0
     |  */
     | object DataAnomalyDetector extends App with APIs {
     | 
     |   import cats._, cats.data._, cats.implicits._
     |   import functions._ // imports the sugarcoated functions for spark sql
     | 
     |   val defaultTracerName = "data-anomaly-tracer"
     |   
     |   val yesterday = System.currentTimeMillis() - 24 * 60 * 1000
     |   val today = System.currentTimeMillis()
     | 
     |   def traceDataAnomalies = {
     |     Either.catchNonFatal{
     |       val (verificationResult1, repo) = 
     |         traceLoadCsvEffectNClose(
     |           sys.env("TMPDIR"),
     |           "src/main/resources/bad_data.csv", // data here is bad....
     |           buildAnomalyDetection(none,
     |                                 yesterday, // represents yesterday
     |                                 Map(),
     |                                 RateOfChangeStrategy(maxRateIncrease = Some(2.0)),
     |                                 Size().some).run)
     |       val (verificationResult2, repo2) = 
     |         traceLoadCsvEffectNClose(
     |           sys.env("TMPDIR"),
     |           "src/main/resources/bad_data2.csv", // data here is worse than bad
     |           buildAnomalyDetection(repo.some,
     |                                 today, // represents today
     |                                 Map(),
     |                                 RateOfChangeStrategy(maxRateIncrease = Some(2.0)),
     |                                 Size().some).run)
     | 
     |       if (verificationResult2.status != Success) {
     |         println("Anomaly detected in the Size() metric!")
     |         getSparkSession(sys.env("TMPDIR")) >>=
     |           {(session: SparkSession) => 
     |               repo2.load().forAnalyzers(Seq(Size())).getSuccessMetricsAsDataFrame(session).show()
     |           }
     |       }
     |       else println("we are done.")
     |  
     |     }
     |   }
     | 
     |   if (!args.headOption.isEmpty && args.headOption.get.equalsIgnoreCase("trace-anomaly"))
     |     traceDataAnomalies
     |   else {
     |     println("You entered no option. Exiting.")
     |     System.exit(-1)
     |   }
     | 
     | }
defined object DataAnomalyDetector
```

