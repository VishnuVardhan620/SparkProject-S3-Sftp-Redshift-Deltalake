package com.pg

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import com.pg.utils.Constants
import org.apache.spark.sql.functions._
object SourceData {

  def main(args: Array[String]): Unit = {
    try {
      val sparkSession = SparkSession.builder.master("local[*]").appName("SftpToS3").getOrCreate()
      sparkSession.sparkContext.setLogLevel(Constants.ERROR)

      val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
      val sftpConfig = rootConfig.getConfig("sftp_conf")

      val s3Config = rootConfig.getConfig("s3_conf")
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

      val finFilePath = s"s3n://${s3Config.getString("s3_bucket")}/ProjectSftpToS3/"

      val sftpDf = sparkSession.read.
        format("com.springml.spark.sftp").
        option("host", sftpConfig.getString("hostname")).
        option("port", sftpConfig.getString("port")).
        option("username", sftpConfig.getString("username")).
        //        option("password", "Temp1234").
        option("pem", sftpConfig.getString("pem")).
        option("fileType", "csv").
        option("delimiter", "|").
        load(s"${sftpConfig.getString("directory")}/receipts_delta_GBR_14_10_2017.csv")
        val sftpDF = sftpDf.withColumn("ins_ts", current_date())

       sftpDF.write.option("header","true").
         partitionBy("mobile_os").
         mode("overwrite").
         parquet(finFilePath)
          val modifiedSftp = sparkSession.read.parquet("s3n://" + Constants.S3_BUCKET + "/ProjectSftpToS3" )
      modifiedSftp.show()
      sftpDf.show()
            sparkSession.close()
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
      }
    }
  }
}