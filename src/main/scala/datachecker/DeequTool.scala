package thalesdigital.io.datachecker

import cats._
import cats.data._
import cats.implicits._

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.profiles.{ColumnProfilerRunner, ColumnProfilerRunBuilder, NumericColumnProfile}

import org.apache.spark.sql._

/**
 * Default trait where all `deequ` APIs might be explored
 *
 * @author Raymond Tay
 * @version 1.0
 */
trait DeequTools {

  /**
   * Profile all columns, should be careful not to profile data columns if you
   * know its going to be more than 30
   * @param df dataframe
   * @return a ColumnProfilerRunBuilder
   */
  def profileAllColumns = Reader{ (df: DataFrame) => new ColumnProfilerRunner().onData(df) }

  /**
   * Executing the profile
   * @param profilter a `ColumnProfilerRunBuilder`
   * @return the result of executing the profiling
   */
  def runProfile = Reader { (profiler: ColumnProfilerRunBuilder) => profiler.run }


  /**
   * Prints statistics about 1 column
   * @param col target col
   * @param df DataFrame
   * @return nothing
   */
  def getStatsForColumn(col: Column) = Reader { (df: DataFrame) =>
    val result = profileAllColumns(df) >>= runProfile.run
    val profileColumn = result.profiles(col.toString).asInstanceOf[NumericColumnProfile]
    println(s"Statistics of '${col.toString}':\n" +
      s"\tminimum: ${profileColumn.minimum.get}\n" +
      s"\tmaximum: ${profileColumn.maximum.get}\n" +
      s"\tmean: ${profileColumn.mean.get}\n" +
      s"\tstandard deviation: ${profileColumn.stdDev.get}\n")
  }

  /**
   * Prints statistics about ALL column
   * @param df DataFrame
   * @return nothing
   */
  def getStatsForAllColumns = Reader { (df: DataFrame) => 
    val result = profileAllColumns(df) >>= runProfile.run
    result.profiles.foreach { case (name, profile) =>
        println(s"Column '$name':\n " +
        s"\tcompleteness: ${profile.completeness}\n" +
        s"\tapproximate number of distinct values: ${profile.approximateNumDistinctValues}\n" +
        s"\tdatatype: ${profile.dataType}\n")
    }
  }
  
}


