
### Demonstration of Profiling Apache Dataframes

![Cats Friendly Badge](https://typelevel.org/cats/img/cats-badge-tiny.png) 

Here is how to use it to profile all the columns of the data

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

The code sample below is extracted from [DataProfiler.scala](../src/main/scala/examples/DataProfiler.scala)
but repeated here for your convenience.

```scala
scala> import thalesdigital.io.app.APIs
import thalesdigital.io.app.APIs

scala> import com.amazon.deequ.checks._
import com.amazon.deequ.checks._

scala> import com.amazon.deequ.constraints._
import com.amazon.deequ.constraints._

scala> import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, Completeness, Mean, Correlation, Compliance, InMemoryStateProvider, Size, DoubleValuedState}
import com.amazon.deequ.analyzers.{Analysis, ApproxCountDistinct, Completeness, Mean, Correlation, Compliance, InMemoryStateProvider, Size, DoubleValuedState}

scala> import thalesdigital.io.deequ._
import thalesdigital.io.deequ._

scala> import org.apache.spark.sql._
import org.apache.spark.sql._

scala> /**
     |  * Run this demo to see how to run profiling leveraging deequ on good datasets.
     |  * There are two effects at play here:
     |  * (a) Obtains all statistics for all columns
     |  * (b) Obtains the statistic for a single column, in this case its "housing_median_age"
     |  *
     |  * @author Raymond Tay
     |  * @version 1.0
     |  */
     | object DataProfiler extends App with APIs {
     | 
     |   import cats._, cats.data._, cats.implicits._
     | 
     |   val defaultTracerName = "profilertracer"
     | 
     |   import functions._ // import ALL the spark sql functions
     | 
     |   Either.catchNonFatal(
     |     loadCsvEffectNClose(
     |       sys.env("TMPDIR"),
     |       "src/main/resources/good_data.csv",
     |       getStatsForAllColumns.run *> getStatsForColumn(col("housing_median_age")).run )
     |   )
     | 
     | }
defined object DataProfiler
```
To run it from the `sbt console`, you need to enter `run` and select this
particular application and once completed, you should see the following:
```
...
19/08/15 18:27:41 INFO DAGScheduler: ResultStage 12 (countByKey at ColumnProfiler.scala:547) finished in 0.010 s
19/08/15 18:27:41 INFO DAGScheduler: Job 6 finished: countByKey at ColumnProfiler.scala:547, took 0.192057 s
Statistics of 'housing_median_age':
	minimum: 1.0
	maximum: 52.0
	mean: 28.639486434108527
	standard deviation: 12.585252725724587

19/08/15 18:27:41 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
19/08/15 18:27:41 INFO MemoryStore: MemoryStore cleared
19/08/15 18:27:41 INFO BlockManager: BlockManager stopped
19/08/15 18:27:41 INFO BlockManagerMaster: BlockManagerMaster stopped
19/08/15 18:27:41 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
19/08/15 18:27:41 INFO SparkContext: Successfully stopped SparkContext
19/08/15 18:27:41 WARN FileSystem: exception in the cleaner thread but it will continue to run
```
