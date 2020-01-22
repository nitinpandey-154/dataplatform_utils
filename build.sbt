import scala.sys.process._

name := "dataplatform_utils"
organization := "com.goibibo"
version := "2.4.4.2"
scalaVersion := "2.11.12"
addCompilerPlugin(scalafixSemanticdb) // enable SemanticDB
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions ++= List(
    "-target:jvm-1.8",
    "-Yrangepos", // required by SemanticDB compiler plugin
    "-Ywarn-unused-import" // required by `RemoveUnused` rule
)
resolvers += "redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"
resolvers += "dynamodb-local-mumbai" at "https://s3.ap-south-1.amazonaws.com/dynamodb-local/release"

libraryDependencies ++= Seq(
    "mysql" % "mysql-connector-java" % "5.1.38",
    "com.whisk" %% "docker-testkit-scalatest" % "0.9.9" % "test",
    "com.whisk" %% "docker-testkit-impl-spotify" % "0.9.9" % "test",
    "com.amazonaws" % "dynamodb-lock-client" % "1.1.0",
    "com.amazonaws" % "aws-java-sdk-core" % "1.11.622",
    /*
     Scala-arm is a dependecy-free and minimalistic library.
     We add this as a dependency because we use it on several of our classes,
     and if we don't include it then almost all our users will have to include it.
     */
    "com.jsuereth" %% "scala-arm" % "2.0",
    "com.thesamet.scalapb" %% "scalapb-runtime" % "0.9.1",
    /*
     We used to include this but now it's part of EMR + Databricks so now it's in provided.
     https://docs.databricks.com/release-notes/runtime/5.3.html#installed-java-and-scala-libraries-scala-2-11-cluster-version
     */
    "org.joda" % "joda-convert" % "1.7" % "provided",
    "org.apache.spark" %% "spark-core" % "2.4.4" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided",
    "org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided",
    "org.apache.spark" %% "spark-catalyst" % "2.4.4" % "provided",
    "org.apache.spark" %% "spark-hive" % "2.4.4" % "provided",
    "joda-time" % "joda-time" % "2.9.3" % "provided",
    "org.slf4j" % "slf4j-api" % "1.7.16" % "provided",
    "com.databricks" %% "spark-redshift" % "3.0.0-preview1" % "provided",
    "org.apache.zookeeper" % "zookeeper" % "3.4.6" % "provided",
    "org.apache.kafka" % "kafka-clients" % "2.0.0" % "provided",
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4" % "provided",
    "org.spark-project.hive" % "hive-metastore" % "1.2.1.spark2" % "provided",
    /*
     Provided because, It will always be available on databricks,
     To run on EMR, add it in her app's sbt file
     */
    "com.databricks" %% "dbutils-api" % "0.0.3" % "provided",
    "io.delta" %% "delta-core" % "0.1.0" % "provided",
    /*
     This is required only for test-cases, so kept in "test".
     */
    "org.apache.hadoop" % "hadoop-common" % "2.9.2" % "test",
    "org.apache.hadoop" % "hadoop-aws" % "2.9.2" % "test",
    /*
     Latest S3 and glue libraries are part of the AWS and databricks cluster and included in the JVM classpath by default.
     */
    "com.amazonaws" % "aws-java-sdk-s3" % "1.11.313" % "provided",
    "com.amazonaws" % "aws-java-sdk-glue" % "1.11.313" % "provided",
    /*
     parquet4s-core is only getting used in the tests.
     */
    "com.github.mjakubowski84" %% "parquet4s-core" % "0.4.0" % "test",
    /*
     Dataplatform utils gets used in several of our internal projects,
     Most of the time projects just need a specific functionality from dataplatform_utils.
     The reason we keep all our fat dependencies as provided is because it allows our dependent apps to
     include the required ones and not bother excluding the ones which are not-required for them.

     For example:
     - SqlShift uses this library just for the locking functionality.
     so it just adds dataplatform_utils + zookeeper as it's dependency.
     - kShift uses this library to create RDD from the Kafka topic and for the distributed lock.
     kShift imports this library and adds kafka + spark-streaming-kafka-0-10 + zookeeper
     as dependency.
     */

    "com.amazon.redshift" % "redshift-jdbc4" % "1.2.20.1043" % "provided" excludeAll (
        ExclusionRule(organization = "com.amazonaws.auth")
        ),
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7" % "provided",
    "software.amazon.awssdk" % "glue" % "2.5.66",
    "software.amazon.awssdk" % "redshift" % "2.5.66",
    "org.json4s" %% "json4s-ast" % "3.5.3" % "provided",
    "org.json4s" %% "json4s-core" % "3.5.3" % "provided",
    "org.json4s" %% "json4s-jackson" % "3.5.3" % "provided",
    "org.scalatest" %% "scalatest" % "3.0.7" % "test",
    /* Pureconfig is only used in test. */
    "com.github.pureconfig" %% "pureconfig" % "0.10.2" % "test",
    "com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.12.0" % "test"
)

parallelExecution in Test := false

dependencyOverrides ++= Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
)

assemblyShadeRules in assembly := Seq(
    // We are building our code against open source delta but shading allows us to run against commercial delta
    ShadeRule
        .rename(
            "org.apache.spark.sql.delta.**" -> "com.databricks.sql.transaction.tahoe.@1"
        )
        .inAll,
    // This clashes with built in libraries in spark. So we shade it.
    ShadeRule.rename("org.apache.http.**" -> "shadedhttp.@1").inAll,
    ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll
)

assemblyMergeStrategy in assembly := {
    case PathList(ps@_*) if ps.last endsWith "versions.properties" =>
        MergeStrategy.concat
    case PathList(ps@_*) if ps.last endsWith ".config" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".json" => MergeStrategy.first
    case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
}
