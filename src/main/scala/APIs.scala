package thalesdigital.io.app

import thalesdigital.io.utilities.SparkTools

import cats._
import cats.data._
import cats.implicits._

import org.apache.spark.sql._

trait APIs extends SparkTools {

  /**
   * Attempts to load the CSV into memory using the workspace provided.
   * Note: SparkSession is not closed explicitly.
   *
   * @param dir working space
   * @param csv path to csv
   */
  def runApi(dir: String, csv: String) = getSparkSession(dir) >>= loadCsv(new java.io.File(csv)).run


  /**
   * Attempts to load the CSV into memory using the workspace provided.
   * Note: SparkSession is closed explicitly.
   *
   * @param dir working space
   * @param csv path to csv
   */
  def runApiNClose(dir: String, csv: String) = getSparkSession(dir) >>= loadCsv2(new java.io.File(csv)).run >>= closeSession.run

  /**
   * Attempts to load the CSV into memory using the workspace provided,
   * processes the loaded CSV data via the function `f`.
   * Note: SparkSession is closed explicitly.
   *
   * @param dir working space
   * @param csv path to csv
   * @param f function that processes the loaded CSV data
   */
  def runApiActionNClose(dir: String, csv: String, f: DataFrame => Unit) = getSparkSession(dir) >>= loadCsv2(new java.io.File(csv)).run >>= closeSessionAfterAction(f).run

}


