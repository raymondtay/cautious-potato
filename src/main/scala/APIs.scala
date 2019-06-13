package thalesdigital.io.app

import thalesdigital.io.utilities._
import thalesdigital.io.datachecker.{DataVerifier, DeequTools}
import thalesdigital.io.tracer._

import cats._
import cats.data._
import cats.implicits._

import org.apache.spark.sql._

import io.opentracing.Scope
import io.opentracing.Span
import io.opentracing.Tracer


import com.amazon.deequ.{VerificationResult, VerificationSuite, VerificationRunBuilder}
import com.amazon.deequ.checks._
import com.amazon.deequ.constraints._
import com.amazon.deequ.analyzers.{Analyzer, Analysis}
import com.amazon.deequ.analyzers.runners.{AnalyzerContext, AnalysisRunner}
import com.amazon.deequ.metrics.{Metric}

/**
 * API implementation
 * 
 * @author Raymond Tay
 * @version 1.0
 */
trait APIs extends SparkTools with DataVerifier with GloblTracer {

  /**
   * Attempts to load the CSV into memory using the workspace provided.
   * Note: SparkSession is not closed explicitly.
   *
   * @param dir working space
   * @param csv path to csv
   */
  def loadCsv(dir: String, csv: String) : DataFrame = getSparkSession(dir) >>= loadCsv(new java.io.File(csv)).run

  def loadCsvT(dir: String, csv: String)(span: Span) : DataFrame = getSparkSession(dir) >>= loadCsv(new java.io.File(csv)).run

  def traceLoadCsv(dir: String, csv: String) = traceRun(loadCsvT)(dir)(csv)("load-csv")

  /**
   * Attempts to load the CSV into memory using the workspace provided.
   * Note: SparkSession is closed explicitly.
   *
   * @param dir working space
   * @param csv path to csv
   */
  def loadCsvNClose(dir: String, csv: String) : Unit = getSparkSession(dir) >>= loadCsv2(new java.io.File(csv)).run >>= closeSession.run

  /**
   * Attempts to load the CSV into memory using the workspace provided,
   * processes the loaded CSV data via the function `f`.
   * Note: SparkSession is closed explicitly.
   *
   * @param dir working space
   * @param csv path to csv
   * @param f function that processes the loaded CSV data
   */
  def loadCsvEffectNClose[A](dir: String, csv: String, f: DataFrame => A) : A = getSparkSession(dir) >>= loadCsv2(new java.io.File(csv)).run >>= closeSessionAfterAction(f).run

  /**
   * Attempts to load the CSV into memory using the workspace provided,
   * processes the loaded CSV data via the function `f`. All actions are
   * traced and reported to the Jaeger server sitting in the network.
   * Note: SparkSession is closed explicitly.
   *
   * @param dir working space
   * @param csv path to csv
   * @param f function that processes the loaded CSV data
   * @param span the active [[Span]] provided to house the metrics
   */
  def loadCsvEffectNCloseT[A](dir: String, csv: String, f: DataFrame => A)(span: Span) : A = {
    logEvent("event", "Load CSV")(span) *>
      getSparkSession(dir) >>=
        logEventWithOutcome[SparkSession]("event", "Obtained SparkSession", span).run *>
          loadCsv2(new java.io.File(csv)).run >>=
            logEventWithOutcome[(DataFrame,SparkSession)]("event", "CSV is loaded into Memory.", span).run *>
              closeSessionAfterAction(f).run >>=
                logEventWithOutcome[A]("event", "CSV is loaded validated by deequ.", span).run
  }

  def traceLoadCsvEffectNClose[A](dir: String, csv: String, f: DataFrame => A) : A =
    traceRun2(loadCsvEffectNCloseT[A])(dir)(csv)(f)("Load CSV Validation Service")

  /**
   * Builds a default VerificationRunBuilder and adds constraints and checks to
   * it with the intention of running the checks against them.
   *
   * Reports the final result
   * @param checks An list or array of [[Check]] and/or [[Constraint]]
   * @param df DataFrame to run them against
   * @return the verification result
   */
  def runDataWithChecks(checks: Check*) : Reader[DataFrame, VerificationResult] =
    Reader{ (df: DataFrame) => checks.foldLeft(defaultVerifier(df))((builder, e) => addConstraint(e).runS(builder).value).run }

  /**
   * Runs the data analysis based on the analyzer passed in
   * @param analyzer an [[Analysis]] object populated with analyzers
   * @param df [[DataFrame]] object
   * @return result
   */
  def runDataWithAnalyzers(analyzer: Analysis) : Reader[DataFrame, Unit] =
    Reader{ (df: DataFrame) => Monad[Id].pure(analyzer.run(df)) >>= logMetrics("MetricsReporter").run }

  /**
   * Builds a default Analysis via the list of Analyzers 
   * Note: It does not run this against the data yet
   *
   * @param alysis the analysis object
   * @param xs the list of analyzers you want to run this data against
   * @return the final Analysis object, after adding these analyzers
   */
  def buildAnalyzers(xs : Analyzer[_, Metric[_]]*) : Reader[Analysis, Analysis] =
    Reader{ (alysis: Analysis) => xs.foldLeft(alysis)((builder, e) => addAnalyzer(e).runS(builder).value) }
}


