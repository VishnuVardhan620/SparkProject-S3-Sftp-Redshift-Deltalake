package com.pg



object SourceDataFinal {

  def main(args: Array[String]): Unit = {
    // reading data using a function
    SourceDataFunction.readFromSftp("receipts_delta_GBR_14_10_2017.csv")
    SourceDataFunction.writeToS3FromSftp("receipts_delta_GBR_14_10_2017.csv","ProjectSftoToS3-2")

  }

}
