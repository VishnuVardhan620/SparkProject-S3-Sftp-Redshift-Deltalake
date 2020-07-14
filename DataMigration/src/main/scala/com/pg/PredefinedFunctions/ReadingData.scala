package com.pg.PredefinedFunctions

import com.pg.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.current_date
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class ReadingData {
  val sparkSession = SparkSession.builder.master("local[*]").appName("SftpToS3").getOrCreate()
  sparkSession.sparkContext.setLogLevel(Constants.ERROR)
  val redshiftConfig = rootConfig.getConfig("redshift_conf")
  val s3Bucket = s3Config.getString("s3_bucket")
  val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
  val sftpConfig = rootConfig.getConfig("sftp_conf")
  val s3Config = rootConfig.getConfig("s3_conf")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
  sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))


  def readSftp(filename: String): DataFrame = {
    var path = s"${sftpConfig.getString("directory")}/"
    val sftpDf = sparkSession.read.
      format("com.springml.spark.sftp").
      option("host", sftpConfig.getString("hostname")).
      option("port", sftpConfig.getString("port")).
      option("username", sftpConfig.getString("username")).
      //        option("password", "Temp1234").
      option("pem", sftpConfig.getString("pem")).
      option("fileType", "csv").
      option("delimiter", "|").
      load(path + filename)
    return sftpDf
  }


  def readParquetS3(filename: String): DataFrame = {
    val parquetDf = sparkSession.read.parquet("s3n://" + Constants.S3_BUCKET + "/" + filename)
    return parquetDf
  }


  def readJsonS3(filename: String): DataFrame = {
    val jsonDf = sparkSession.read
      .json("s3n://" + Constants.S3_BUCKET + "/" + filename)
    return jsonDf

  }


  def readcsvS3(filename: String): DataFrame = {
    val csvDf = sparkSession.read
      .option("mode", "DROPMALFORMED") // PERMISSIVE OR FAILFAST
      .option("header", "false")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv("s3n://" + Constants.S3_BUCKET + "/" + filename)
    return csvDf
  }

  def writeParquetToS3(df: DataFrame, filename: String) = {
    df.write.option("header", "true").
      partitionBy("mobile_os").
      mode("overwrite").
      parquet("s3n://" + Constants.S3_BUCKET + "/" + filename)
  }


  def writeToRedshift(df: DataFrame,) = {
    val jdbcUrl = Constants.getRedshiftJdbcUrl(redshiftConfig)
    df.write
      .format("com.databricks.spark.redshift")
      .option("url", jdbcUrl)
      .option("tempdir", s"s3n://${s3Bucket}/temp")
      .option("forward_spark_s3_credentials", "true")
      .option("dbtable", "PUBLIC.TXN_FCT")
      .mode(SaveMode.Overwrite)
      .save()
    println("Completed   <<<<<<<<<")
  }

}