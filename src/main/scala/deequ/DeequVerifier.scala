package thalesdigital.io.deequ

import cats._
import cats.data._
import cats.implicits._

import com.amazon.deequ.{VerificationSuite, VerificationRunBuilder}
import com.amazon.deequ.checks._
import com.amazon.deequ.constraints._
import com.amazon.deequ.profiles.{ColumnProfilerRunner, ColumnProfilerRunBuilder, NumericColumnProfile}
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.{Analyzer, Analysis}
import com.amazon.deequ.metrics.{Metric}


import org.apache.spark.sql._

/**
 * Data Verification Phase where there are lots of combinators which can be
 * combined together to form high-order combinator.
 *
 *
 * @author Raymond Tay
 * @version 1.0
 */
trait DeequVerifier {

  /**
   * Builds a default Verification Suite Runner which you can use to build
   * higher-order combinators.
   *
   * @param df DataFrame
   * @return an instance of the [[VerificationRunBuilder]]
   */
  def defaultVerifier : Reader[DataFrame, VerificationRunBuilder] =
    Reader{
      (df: DataFrame) =>
        var builder = VerificationSuite().onData(df)
        builder
    }

  /**
   * State machine used for building up the checks.
   *
   * @param check either a [[Check]] or [[Constraint]]
   * @param builder an instance of the builder you are trying to leverage by
   * "adding" checks and constraints to it.
   */
  def addConstraint(check: Check) : State[VerificationRunBuilder, VerificationRunBuilder] =
    State{ (builder: VerificationRunBuilder) => 
      (builder.addCheck(check), builder)
    }

  /**
   * A state machine to allow you to keep adding analyzers
   * @param anlysis Analyzer
   * @return 
   */
  def addAnalyzer(analyzer : Analyzer[_, Metric[_]]) : State[Analysis, Analysis] =
    State { (builder: Analysis) => 
      (builder.addAnalyzer(analyzer), builder)
    }

}


