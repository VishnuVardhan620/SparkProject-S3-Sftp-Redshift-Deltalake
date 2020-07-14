package com.pg.SftptoS3

import com.pg.SourceDataLoading

object SourceDataFinal {

  def main(args: Array[String]): Unit = {
    // reading data using a function
    SourceDataLoading.readFromSftp("receipts_delta_GBR_14_10_2017.csv")
    SourceDataLoading.writeToS3FromSftp("receipts_delta_GBR_14_10_2017.csv","ProjectSftoToS3-2")

  }

}
