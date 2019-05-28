package thalesdigital.io.datachecker

import cats._
import cats.data._
import cats.implicits._

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.profiles.{ColumnProfilerRunner, ColumnProfilerRunBuilder, NumericColumnProfile}

import org.apache.spark.sql._

// Default trait where all `deequ` APIs might be explored
//
trait DeequTools {

  def profileAllColumns = Reader{ (df: DataFrame) => new ColumnProfilerRunner().onData(df) }

  def runProfile = Reader { (profiler: ColumnProfilerRunBuilder) => profiler.run }

}


