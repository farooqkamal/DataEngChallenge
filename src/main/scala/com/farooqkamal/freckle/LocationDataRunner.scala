package com.farooqkamal.freckle

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object LocationDataRunner extends App
  with Serializable
  with Loggable
  with LocationDataReader
  with LocationDataAnalyzer {

  val logger = LoggerFactory.getLogger("location")

  run()

  def createSparkContext: SparkSession = {
      SparkSession
        .builder()
        .appName("LocationData")
        .config("spark.master", "local")
        .config("spark.testing", "true")
        .getOrCreate()
  }

  def run(): Unit = {

    implicit val spark: SparkSession = createSparkContext

    val glob = Option(System.getProperty("glob")) getOrElse "*.log-*"
    val dirPath = System.getProperty("dirPath")
    require(dirPath != null, "Source data path argument -DdirPath not defined")

    val locDf = loadDataFromDirectory(dirPath, glob).cache()
    logger.info("Loaded DataFrame ...")

    val statsDf = getStats(locDf)
    logger.info("Location Stats ...")
    statsDf.show()

    val hashDf = computeGeoHashes(locDf).cache()
    logger.info("Recomputed Geo Hashes on DataFrame UDF ...")
    hashDf.show()

    val idfaClusterDf = computeIdfaClusters(hashDf)
    logger.info("Computed IDFA clusters based in on GeoHashes for max distance under ±610 meter ...")
    idfaClusterDf.show()

    val topCityDf = computeTopCities(hashDf)
    logger.info("topCityDf ...")
    topCityDf.show()

    val hourEventsDf = computePopularHours(hashDf)
    logger.info("hourEventsDf ...")
    hourEventsDf.show()
  }

  protected def writeToFile(path: String, geoHashDf: DataFrame): Unit = geoHashDf.write.parquet(path)

}

trait Loggable {
  def logger: Logger
}
