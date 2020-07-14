package com.pg

import com.pg.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_date

object SourceDataLoading {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val sftpConfig = rootConfig.getConfig("sftp_conf")
    readFromSftp("receipts_delta_GBR_14_10_2017.csv")


  }

  def readFromSftp(filename:String) = {
    val sparkSession = SparkSession.builder.master("local[*]").appName("SftpToS3").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)


    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val sftpConfig = rootConfig.getConfig("sftp_conf")
    val s3Config = rootConfig.getConfig("s3_conf")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))
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
    sftpDf.show()
  }
    def writeToS3FromSftp(filename:String, foldername:String)={
      val sparkSession = SparkSession.builder.master("local[*]").appName("SftpToS3").getOrCreate()
      sparkSession.sparkContext.setLogLevel(Constants.ERROR)

      val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
      val sftpConfig = rootConfig.getConfig("sftp_conf")
      val s3Config = rootConfig.getConfig("s3_conf")
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))
      var path = s"${sftpConfig.getString("directory")}/"
      val S3filepath = s"s3n://${s3Config.getString("s3_bucket")}/"+foldername+"/"
      val sftpDf = sparkSession.read.
        format("com.springml.spark.sftp").
        option("host", sftpConfig.getString("hostname")).
        option("port", sftpConfig.getString("port")).
        option("username", sftpConfig.getString("username")).
        //        option("password", "Temp1234").
        option("pem", sftpConfig.getString("pem")).
        option("fileType", "csv").
        option("delimiter", "|").
        load(path+filename)
      val sftpDF = sftpDf.withColumn("ins_ts", current_date())
       sftpDF.write.option("header","true").
        partitionBy("mobile_os").
        mode("overwrite").
        parquet(S3filepath)

      val modifiedSftp = sparkSession.read.parquet("s3n://" + Constants.S3_BUCKET + "/ProjectSftpToS3" )
      modifiedSftp.show()
    }


  }
