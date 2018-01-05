package com.farooqkamal.freckle

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, _}

trait LocationDataAnalyzer {
  self: Serializable with Loggable =>

  def getStats(locDf: DataFrame): DataFrame = {
    val countDf = locDf
      .groupBy("idfa")
      .count
      .sort(col("count").desc)

    countDf.describe("count")
  }

  private val computeGeoHash: (Double, Double) => String =
    (lat, lng) => GeoHash.withCharacterPrecision(lat, lng, 12).toBase32

  private val geoHashUdf: UserDefinedFunction = udf(computeGeoHash)

  def computeGeoHashes(locDf: DataFrame): DataFrame =
    locDf
      .withColumn("geohash", geoHashUdf(col("lat"), col("lng")))
      .filter(col("geohash") =!= lit("s00000000000")) // Seems like a default or n/a value

  def computeIdfaClusters(geoHashDf: DataFrame): DataFrame =
    geoHashDf
      .withColumn("geohash_partial", substring(col("geohash"), 0, 6)) // 6 ~  ±0.61 km |  7 ~ ±0.076 km
      .groupBy("geohash_partial")
      .agg(countDistinct("idfa").as("idfa_count"))
      .orderBy(col("idfa_count").desc)

  def computeTopCities(geoHashDf: DataFrame): DataFrame =
    geoHashDf
      .groupBy(col("city"))
      .agg(countDistinct("idfa").as("count"))
      .orderBy(col("count").desc)

  private val hourRange = (0 to 23).toList

  def computePopularHours(geoHashDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val geoHashHourWithCountFullDf = computeHourPercentiles(geoHashDf)
    pivotHours(geoHashHourWithCountFullDf)
  }

  protected def computeHourPercentiles(geoHashDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val geoHashHourDf = geoHashDf
      .withColumn("hour", hour(col("event_time").cast(TimestampType)))
      .groupBy("hour")
      .agg(count("hour").as("hour_count"), countDistinct("idfa").as("user_count"))
      .cache()

    val totalRow = geoHashHourDf.select(sum(col("hour_count")), sum(col("user_count"))).collect().head
    val (hourCntTotal, userCntTotal) = (totalRow.getLong(0), totalRow.getLong(1))

    val geoHashHourWithCountDf = geoHashHourDf
      .groupBy("hour", "hour_count", "user_count")
      .agg(functions.round(col("hour_count") / hourCntTotal * 100, 2).as("hour_percentile"),
        functions.round(col("user_count") / userCntTotal * 100, 2).as("user_percentile"))

    val allHoursRdd = spark.sparkContext.parallelize(hourRange.map(Row(_)))
    val hourDf = spark.createDataFrame(allHoursRdd, StructType(Seq(StructField("hour", IntegerType))))

    hourDf.join(geoHashHourWithCountDf, Seq("hour"), "left_outer")
      .na.fill(0)
      .orderBy(col("hour").asc)
  }

  protected def pivotHours(geoHashHourDf: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val statsArr = geoHashHourDf.select(
      col("hour_count").cast(StringType),
      col("hour_percentile").cast(StringType),
      col("user_count").cast(StringType),
      col("user_percentile").cast(StringType)
    ).collect

    val hrCountRow = Row("event_count" :: statsArr.map(_.getString(0)).toList: _*)
    val hrPercRow = Row("event_percentile" :: statsArr.map(_.getString(1)).toList: _*)
    val userCountRow = Row("user_count" :: statsArr.map(_.getString(2)).toList: _*)
    val userPercRow = Row("user_percentile" :: statsArr.map(_.getString(3)).toList: _*)

    val rdd = spark.sparkContext.parallelize(Seq(hrCountRow, hrPercRow, userCountRow, userPercRow))

    val structList = hourRange map { hour => StructField(hour.toString, StringType)}
    val hourStruct = StructType(StructField("hour", StringType) :: structList)

    spark.createDataFrame(rdd, hourStruct)
  }

}
