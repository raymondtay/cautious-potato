package thalesdigital.io.utilities

import cats._
import cats.data._
import cats.implicits._
import scala.concurrent._
import scala.concurrent.duration._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, Row}

/**
 * Everything related to Apache Spark ☺ ; abstractions are expressed as Monads
 * @author Raymond Tay
 * @version 1.0
 */
trait SparkTools {

  def getSparkSession : Kleisli[Id, String, SparkSession] = Kleisli{ (workingSpace: String) => 
    val session = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    Monad[Id].pure(session)
  }

  /**
   * Loads the csv as a DataFrame and returns a product type
   * @param pathToFile
   * @return 
   */
  def loadCsv(pathToFile: java.io.File) : Kleisli[Dataset, Id[SparkSession], Row] =
    Kleisli{ (session: SparkSession) => 
      session.read.format("csv").option("header", true).load(pathToFile.getAbsolutePath)
    }

  /**
   * Loads the csv as a DataFrame and returns a product type
   * @param pathToFile
   * @return a 2-tuple
   */
  def loadCsv2(pathToFile: java.io.File) =
    Kleisli{ (session: SparkSession) => 
      Monad[Id].product(
       session.read.format("csv").option("header", true).load(pathToFile.getAbsolutePath),
       session)
    }

  /**
   * Closes the [[SparkContext]] immediately
   * @param 2-tuple
   * @return Unit
   */
  def closeSession = Reader{ (pair: (DataFrame, SparkSession)) => pair._2.close() }

  /**
   * Closes the [[SparkContext]] immediately after running the action "f".
   * @param f a function which consumes a [[DataFrame]] and returns a result of type [[A]].
   * @return Unit
   */
  def closeSessionAfterAction[A](f: DataFrame => A) : Reader[(DataFrame,SparkSession), A] =
    Reader{ (pair: (DataFrame, SparkSession)) =>
      Applicative[Id].lift((df: DataFrame) => f(df))(pair._1) >>= Monad[Id].lift{(result: A) => pair._2.close(); result}
    }

}

