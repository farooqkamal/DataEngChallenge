package com.farooqkamal.freckle

import java.nio.file.Paths

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

class LocationDataSpec extends BaseSpec with LocationDataReader with LocationDataAnalyzer {

  var loadedDf: DataFrame = _

  "Freckle location data main routine" should {

    "validate data loaded" in {
      val resourceDir = Paths.get("src/test/resources").toAbsolutePath.toString
      loadedDf = loadDataFromDirectory(resourceDir, "*.json")(spark)
      loadedDf must not be null
    }

    "validate percentile total for hourly stats" in {
      val df = computeHourPercentiles(loadedDf)(spark)
      val row = df
        .select(sum(col("hour_percentile").cast(DoubleType)), sum(col("user_percentile").cast(DoubleType))).collect.head

      row.getDouble(0) mustBe 100.00
      row.getDouble(1) mustBe 100.00
    }
  }

}
