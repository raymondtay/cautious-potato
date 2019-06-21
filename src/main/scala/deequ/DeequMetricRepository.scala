package thalesdigital.io.deequ

import cats._, data._, implicits._

import scala.io._
import java.io.File
import com.google.common.io.Files
import org.apache.spark.sql.SparkSession
import com.amazon.deequ._
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
   * it for processing again.
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

}

trait ResultKeyBuilder {
  def run(prop: Map[String,String]) =
    ResultKey(System.currentTimeMillis(), prop)
}


