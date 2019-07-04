import Dependencies.myLibraryDependencies

name := "deequ_project"

scalaVersion := "2.11.9"

version := "1.0"

libraryDependencies ++= myLibraryDependencies

scalacOptions += "-Ypartial-unification"

enablePlugins(TutPlugin)

tutTargetDirectory := baseDirectory.value / "docs"

