import Dependencies.myLibraryDependencies
import sbtsonar.SonarPlugin.autoImport.sonarProperties

name := "deequ_project"

scalaVersion := "2.11.9"

version := "1.0"

libraryDependencies ++= myLibraryDependencies

scalacOptions += "-Ypartial-unification"
scalacOptions += "-deprecation"

enablePlugins(TutPlugin)

tutTargetDirectory := baseDirectory.value / "docs"

// [sonar.projectName] is a placeholder for the sonarqube UI name (caution: if
// you change the name some time after the data is collected, i don't know if
// you can still view the data before the changes were applied.)
// [sonar.projectKey] is the key that really matters.
sonarProperties ++= Map(
  "sonar.projectName" -> "CautiousPotato",
  "sonar.projectKey" -> "33072aa7977da1454c42e825a3ded1bc25d2b002",
  "sonar.host.url" -> "http://localhost:19000",
  "sonar.sources" -> "src/main/scala",
  "sonar.tests" -> "src/test/scala",
  "sonar.scala.scoverage.reportPath" -> "target/scala-2.11/scoverage-report/scoverage.xml")

