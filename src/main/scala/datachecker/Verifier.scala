package thalesdigital.io.datachecker

import cats._
import cats.data._
import cats.implicits._

import com.amazon.deequ.{VerificationSuite, VerificationRunBuilder}
import com.amazon.deequ.checks._
import com.amazon.deequ.constraints._
import com.amazon.deequ.profiles.{ColumnProfilerRunner, ColumnProfilerRunBuilder, NumericColumnProfile}

import org.apache.spark.sql._

/**
 * Data Verification Phase where there are lots of combinators which can be
 * combined together to form high-order combinator.
 *
 *
 * @author Raymond Tay
 * @version 1.0
 */
trait DataVerifier {

  val defaultVerifier : Reader[DataFrame, VerificationRunBuilder] =
    Reader{
      (df: DataFrame) =>
        var builder = VerificationSuite().onData(df)
        builder
    }
 
  //def addConstraints = Reader{ (runner: VerificationRunBuilder) => 
  //  
  //}

  def addConstraint(check: Check) : State[VerificationRunBuilder, VerificationRunBuilder] =
    State{ (builder: VerificationRunBuilder) => 
      (builder.addCheck(check), builder)
    }

}


