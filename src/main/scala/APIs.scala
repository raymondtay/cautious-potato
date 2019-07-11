package thalesdigital.io.app

import thalesdigital.io.utilities._
import thalesdigital.io.deequ.{DeequVerifier, DeequProfile, DeequMetricRepository, ResultKeyBuilder}
import thalesdigital.io.tracer._

import cats._
import cats.data._
import cats.implicits._

import org.apache.spark.sql._

import io.opentracing.Scope
import io.opentracing.Span
import io.opentracing.Tracer


import com.amazon.deequ.{VerificationResult, VerificationSuite, VerificationRunBuilderWithRepository, VerificationRunBuilder}
import com.amazon.deequ.repository._
import com.amazon.deequ.anomalydetection._
import com.amazon.deequ.repository.memory._
import com.amazon.deequ.checks._
import com.amazon.deequ.checks.CheckStatus._
import com.amazon.deequ.constraints._
import com.amazon.deequ.analyzers.{Analyzer, Analysis, State => DState}
import com.amazon.deequ.analyzers.runners.{AnalyzerContext, AnalysisRunner}
import com.amazon.deequ.metrics.{Metric}

/**
 * API implementation
 * 
 * @author Raymond Tay
 * @version 1.0
 */
trait APIs extends SparkTools
            with DeequVerifier
            with DeequProfile
            with DeequMetricRepository
            with GloblTracer {

  /**
   * Attempts to load the CSV into memory using the workspace provided.
   * Note: SparkSession is not closed explicitly.
   *
   * @param dir working space
   * @param csv path to csv
   */
  def loadCsv(dir: String, csv: String) : DataFrame = getSparkSession(dir) >>= loadCsv(new java.io.File(csv)).run

  def loadCsvT(dir: String, csv: String, span: Span) : DataFrame = getSparkSession(dir) >>= loadCsv(new java.io.File(csv)).run

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
  def loadCsvEffectNCloseT[A](dir: String, csv: String, span: Span, f: DataFrame => A) : A = {
    logEvent("event", "Load CSV")(span) *>
      getSparkSession(dir) >>=
        logEventWithOutcome[SparkSession]("event", "Obtained SparkSession", span).run *>
          loadCsv2(new java.io.File(csv)).run >>=
            logEventWithOutcome[(DataFrame,SparkSession)]("event", "CSV is loaded into Memory.", span).run *>
              closeSessionAfterAction(f).run >>=
                logEventWithOutcome[A]("event", "CSV is loaded validated by deequ.", span).run
  }

  /**
   * Trace the execution from the obtaining of the Spark session object,
   * loading the CSV datafile to the part where a side-effect function is being
   * executed.
   *
   * @param dir working directory
   * @param csv path to the CSV data file
   * @param f side-effect function to run
   * @return
   */
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
   * Sends the [[VerificationResult]] to a external Logging facility
   * Note: The data records are flattened and sent out to the logger
   *       so that the developer can understand the details.
   * @param vr results of the verification process
   * @return
   */
  def sendVerificationResultToLogstore(implicit tracer : Tracer) =
    Reader { (vr: VerificationResult) => 
      getSparkSession(sys.env("TMPDIR")) >>=
        ((session: SparkSession) => VerificationResult.successMetricsAsDataFrame(session, vr)) >>=
          ((df: DataFrame) => {
            val cSpan : Span = tracer.buildSpan("VerificationResult").asChildOf(tracer.activeSpan).start
            df.show()
            val resultsForAllConstraints = vr.checkResults.flatMap { case (_, checkResult) => checkResult.constraintResults }
            var buffer = collection.mutable.ListBuffer.empty[String]
            resultsForAllConstraints
              .filter { _.status != ConstraintStatus.Success }
              .foreach { result => buffer += s"${result.constraint}: ${result.message.get}" }

            logEvent("Result of Data Verification", buffer.toList.mkString("\n"))(cSpan)
            cSpan.finish
          })
          vr
    }

  /**
   * Runs the data analysis based on the analyzer passed in
   * @param analyzer an [[Analysis]] object populated with analyzers
   * @param df [[DataFrame]] object
   * @return result
   */
  def runDataWithAnalyzers(analyzer: Analysis) : Reader[DataFrame, Unit] =
    Reader{ (df: DataFrame) => Monad[Id].pure(analyzer.run(df)) >>= logMetrics("MetricsReporter").run }

  /**
   * Builds a default VerificationRunBuilder and adds constraints and checks to
   * it with the intention of running the checks against them.
   *
   * Reports the final result
   * @param checks An list or array of [[Check]] and/or [[Constraint]]
   * @param df DataFrame to run them against
   * @return the verification result
   */
  def runDataWithChecksNStorage(metricFileName: String, prop: Map[String,String], checks: Check*) : Reader[DataFrame, VerificationResult] =
    Reader{ (df: DataFrame) =>
      val builder = checks.foldLeft(defaultVerifier(df))((builder, e) => addConstraint(e).runS(builder).value)
      val verifier =
        getSparkSession(sys.env("TMPDIR")) >>=
          useLocalMetricRepository(metricFileName).run >>=
            useRepositoryOnRunner.run
    
      (verifier.runA(builder) >>= saveMetricsToRepository(prop)(new ResultKeyBuilder{val time: Long = System.currentTimeMillis}).runS).value.run
    }

  /**
   * The idea is to have a state machine which allows the developer to compose
   * a computation that leverages the [[MetricsRepository]].
   *
   * @param time
   * @param prop 
   * @param strategy
   * @analyzer
   */
  def buildAnomalyDetection[S <: DState[S]](someMetricsRepo: Option[MetricsRepository],
                                            timeKey : Long,
                                            prop: Map[String,String],
                                            strategy: AnomalyDetectionStrategy,
                                            analyzer: Option[Analyzer[S, Metric[Double]]]) : Reader[DataFrame, (VerificationResult, MetricsRepository)] =
    Reader{ (df: DataFrame) =>
      val vr : VerificationRunBuilder = defaultVerifier(df)
      def F : Reader[MetricsRepository, (VerificationResult,MetricsRepository)] =
        Reader{(repo: MetricsRepository) =>
          val f =
              useRepositoryOnRunner(repo).runA(vr) >>=
                saveMetricsToRepository(prop)(new ResultKeyBuilder{val time : Long = timeKey}).runA >>=
                  addAnomalyChecks(analyzer)(strategy).runA
          (f.value.run, repo)
        }
     
      if (someMetricsRepo.nonEmpty) F.run(someMetricsRepo.get)
      else F.run(new InMemoryMetricsRepository())
    }

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


