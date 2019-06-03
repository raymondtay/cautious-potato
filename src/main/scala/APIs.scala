package thalesdigital.io.app

import thalesdigital.io.utilities.SparkTools
import thalesdigital.io.datachecker.{DataVerifier, DeequTools}

import cats._
import cats.data._
import cats.implicits._

import org.apache.spark.sql._

import com.amazon.deequ.{VerificationResult, VerificationSuite, VerificationRunBuilder}
import com.amazon.deequ.checks._
import com.amazon.deequ.constraints._

/**
 * API implementation
 * 
 * @author Raymond Tay
 * @version 1.0
 */
trait APIs extends SparkTools with DataVerifier {

  /**
   * Attempts to load the CSV into memory using the workspace provided.
   * Note: SparkSession is not closed explicitly.
   *
   * @param dir working space
   * @param csv path to csv
   */
  def loadCsv(dir: String, csv: String) : DataFrame = getSparkSession(dir) >>= loadCsv(new java.io.File(csv)).run


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

}


