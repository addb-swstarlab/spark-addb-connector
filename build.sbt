name := "addb-srconnector"

version := "1.0"

scalaVersion := "2.11.8"

// SBT can search local Maven repo
//resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
resolvers += "Atlassian Releases" at "https://maven.atlassian.com/public/"
resolvers += "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

// jedis dependency
libraryDependencies += "kr.ac.yonsei.delab" % "addb-jedis" % "0.0.2"
// Spark dependencies
libraryDependencies ++= Seq (
	"org.apache.spark" %% "spark-sql" % "2.0.2",
	"org.apache.spark" %% "spark-core" % "2.0.2"
)
// pool dependency
//libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.4.3"
// other source for erasing warnings
dependencyOverrides ++= Set (
	"com.google.guava" %% "guava" % "14.0.1",
	"commons-net" %% "commons-net" % "2.2"
)
// test dependencies
libraryDependencies ++= Seq (
	"org.scalatest" % "scalatest_2.11" % "2.2.1" % "test"
)
// log
libraryDependencies ++= Seq (
	"org.slf4j" % "slf4j-log4j12" % "1.6.1",
	"org.apache.logging.log4j" % "log4j-api" % "2.11.0",
	"org.apache.logging.log4j" % "log4j-core" % "2.11.0"
)
// When execute "sbt assembly" command,
// it resolves many errors...
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
