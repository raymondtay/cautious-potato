package thalesdigital.io.app


import thalesdigital.io.datachecker.DeequTools
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, Row}

object Main extends App with APIs with DeequTools {

  import cats._, cats.data._, cats.implicits._
  var dataset : DataFrame = _

  /* Uncomment the following iff you just want to load the csv, resources
   * released implicitly. */
  //Either.catchNonFatal(
  //  runApiNClose(sys.env("TMPDIR"), "src/main/resources/housing.csv")
  //)

  Either.catchNonFatal(
    runApiActionNClose(sys.env("TMPDIR"), "src/main/resources/good_data.csv", action)
  )

  def action = { (df: DataFrame) => 
    dataset = df
    val result = profileAllColumns(dataset) >>= runProfile.run
    result.profiles.foreach { case (name, profile) =>
        println(s"Column '$name':\n " +
        s"\tcompleteness: ${profile.completeness}\n" +
        s"\tapproximate number of distinct values: ${profile.approximateNumDistinctValues}\n" +
        s"\tdatatype: ${profile.dataType}\n")
    }
  }

}

