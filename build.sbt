name := """TopNrankPureSLICK"""

version := "1.0"

scalaVersion := "2.10.3"

mainClass in Compile := Some("nl.lambdalloyd.slickeval.TopNrankPureSLICK")

val additionalClasses = file("/lib/ojdbc7.jar")

unmanagedClasspath in Test += additionalClasses

libraryDependencies ++= List(
  "com.h2database" % "h2" % "1.4.177",
  "com.typesafe.slick" %% "slick-extensions" % "2.0.0",
  "com.typesafe.slick" %% "slick" % "2.0.1",
  "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test",
  "org.slf4j" % "slf4j-nop" % "1.6.4"
)

scalacOptions in Compile ++= Seq("-doc-root-content", "README.md")

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/"

