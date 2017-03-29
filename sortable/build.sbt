name := "ClueSearch"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq (

  "org.apache.spark"          %% "spark-core"   %    "2.0.1" % "provided",
  "org.apache.spark"          %% "spark-sql"    %    "2.0.1" % "provided",
  "org.apache.spark"          %% "spark-mllib"  %    "2.0.1" % "provided",
  "org.apache.spark"          %% "spark-hive"   %    "2.0.1" % "provided",
  "com.databricks"            %% "spark-csv"    %    "1.2.0",
  "org.scalaj"                %% "scalaj-http" %     "2.3.0"
)
