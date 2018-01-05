package com.farooqkamal.freckle

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

trait LocationDataReader {
  self: Serializable with Loggable =>

  def loadDataFromDirectory(dirPath: String, glob: String)(implicit spark: SparkSession): DataFrame = {

    logger.info(s"Reading files from directory path $dirPath")
    val dirStream = Files.newDirectoryStream(Paths.get(dirPath), glob).toList

    val loadedDfs = dirStream map { path => loadDataFromSingleFile(path.toAbsolutePath.toString) }
    loadedDfs.reduce(_ union _)
  }

  def loadDataFromSingleFile(filePath: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading data from $filePath ...")

    spark.read.json(filePath)
      .withColumn("geohash", lit(null)) // empty out the current geohash and compute later
  }
}
