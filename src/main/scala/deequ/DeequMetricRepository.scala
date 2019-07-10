package thalesdigital.io.deequ

import cats._, data._, implicits._

import scala.io._
import java.io.File
import com.google.common.io.Files
import org.apache.spark.sql.SparkSession
import com.amazon.deequ._
import com.amazon.deequ.analyzers.{Analyzer, State => DState}
import com.amazon.deequ.anomalydetection._
import com.amazon.deequ.metrics.{Metric}
import com.amazon.deequ.repository._
import com.amazon.deequ.repository.fs._


/**
 * Convenient functions for storing data to metrics repository
 * @author Raymond Tay
 * @version 1.0
 */
trait DeequMetricRepository {

  /**
   * Prepares the metric data to be stored
   * @param fileName name of file; remember its a JSON file
   * @return repository
   */
  def useLocalMetricRepository(fileName : String) =
    Reader{ (sparkSession : SparkSession) => 
      val metricsFile = new File(Files.createTempDir(), fileName)
      val repository = FileSystemMetricsRepository(sparkSession, metricsFile.getAbsolutePath)
      repository
    }

  /**
   * Augments the verifier with a [[MetricsRepository]]
   * @param mRepository
   * @param verifier
   * @return State function to retrieve
   */
  def useRepositoryOnRunner : Reader[MetricsRepository, State[VerificationRunBuilder, VerificationRunBuilderWithRepository]] =
    Reader { (mRepository: MetricsRepository) =>
      State{ (builder: VerificationRunBuilder) =>
        (builder, builder.useRepository(mRepository))
      }
    }

  /**
   * Augments the verifier with a [[ResultKey]] so that you can easily retrieve
   * it for processing again; the key will be generated based on the current
   * timestamp.
   * @param mRepository
   * @param verifier
   * @return State function to retrieve
   */
  def saveMetricsToRepository(prop: Map[String,String]) =
    Reader {(resultKeyBuilder: ResultKeyBuilder) => 
      State { (builder: VerificationRunBuilderWithRepository) =>
        val resultKey = resultKeyBuilder.run(prop)
        (builder.saveOrAppendResult(resultKey), builder)
      }
    }

  /**
   * Add anomaly checks to the analysis
   * @param analyzer 
   * @param builder
   * @return State function
   */
  def addAnomalyChecks[S <: DState[S]](analyzer: Option[Analyzer[S, Metric[Double]]]) =
    Reader{ (strategy: AnomalyDetectionStrategy) => 
      State{ (builder: VerificationRunBuilderWithRepository) =>
        (builder, builder.addAnomalyCheck(strategy, analyzer.get))
      }
    }
}

trait ResultKeyBuilder {
  // `time` is the system time
  val time : Long

  // the key-value pairs that is needed by deequ to mark the data attributes
  def run(prop: Map[String,String]) =
    ResultKey(time, prop)
}


