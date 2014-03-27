name := """TopNrankPureSLICK"""

version := "1.0"

scalaVersion := "2.10.3"

mainClass in Compile := Some("nl.lambdalloyd.TopNrankPureSLICK")

libraryDependencies ++= List(
  "com.typesafe.slick" %% "slick" % "2.0.1",
  "org.slf4j" % "slf4j-nop" % "1.6.4",
  "com.h2database" % "h2" % "1.3.175",
  "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test",
  "com.typesafe.slick" %% "slick-extensions" % "2.0.0"
)

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/"

