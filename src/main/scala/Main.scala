package thalesdigital.io.app


import thalesdigital.io.datachecker.DeequTools
import org.apache.spark.sql._

object Main extends App with APIs with DeequTools {

  import cats._, cats.data._, cats.implicits._

  /* Uncomment the following iff you just want to load the csv, resources
   * released implicitly. */
  // Either.catchNonFatal(
  //   runApiNClose(sys.env("TMPDIR"), "src/main/resources/housing.csv")
  // )

  import functions._
  Either.catchNonFatal(
    runApiActionNClose(
      sys.env("TMPDIR"),
      "src/main/resources/good_data.csv",
      getStatsForAllColumns.run *> getStatsForColumn(col("housing_median_age")).run )
  )


}

