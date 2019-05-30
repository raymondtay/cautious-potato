package thalesdigital.io.utilities

import cats._
import cats.data._
import cats.implicits._

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, Row}

// Everything related to Apache Spark ☺ ; abstractions are expressed as Monads
//
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

  def closeSession = Reader{ (pair: (DataFrame, SparkSession)) => pair._2.close() }

  def closeSessionAfterAction(f: DataFrame => Unit) =
    Reader{ (pair: (DataFrame, SparkSession)) => f(pair._1); pair._2.close() }

}

