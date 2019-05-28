import sbt._

// A idiomatic way of abstracting and cleaning up the build file - more Scala like ;)
object Dependencies {

  val sparkVersion  = "2.4.3"
  val specs2Version = "4.3.4"

  object SparkDependencies {

    val sparkCore  = "org.apache.spark" %% "spark-core"  % sparkVersion
    val sparkSql   = "org.apache.spark" %% "spark-sql"   % sparkVersion
    val sparkMLLib = "org.apache.spark" %% "spark-mllib" % sparkVersion

    val sparkLibs = sparkCore :: sparkSql :: sparkMLLib :: Nil

    // From this point onwards, you can apply the pattern to configure for other scopes ;) 
    val sparkLibsProvided = sparkLibs.map(_ % "provided")
    val sparkLibsTest = sparkLibs.map(_ % "test")
    val sparkLibsCompile = sparkLibs.map(_ % "compile")

  }
  
  val typesafeConfigVersion = "1.3.3"
  val catsCoreVersion       = "2.0.0-M1"
  val deequVersion          = "1.0.1"

  val catsCoreLib          = "org.typelevel" %% "cats-core" % catsCoreVersion
  val deequLib             = "com.amazon.deequ" % "deequ" % deequVersion
  val typesafeConfig       = "com.typesafe"  % "config"             % typesafeConfigVersion
  val specs2Lib            = "org.specs2"   %% "specs2-core"        % specs2Version % Test
  val specs2Libscalacheck  = "org.specs2"   %% "specs2-scalacheck"  % specs2Version % Test

  import SparkDependencies._

  val myLibraryDependencies =
    sparkLibs ++
      (typesafeConfig ::
       specs2Libscalacheck ::
       specs2Lib ::
       deequLib ::
       catsCoreLib ::
       Nil)

}

