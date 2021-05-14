package com.atip.monitoring

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * Hello world!
 *
 */
object Prof {

  def main(args: Array[String]): Unit = {
    //set the varaibles
    val process_date = args(0)//"20200610"//
    val raw_path = "/otds_int_atip/raw/atip/geo8/"
    val stan_Path = "/otds_int_atip/prepared/standardized/atip/recognized/"

    //creating spark session
    val spark = SparkSession.builder().appName("ATIP_PROF_TESTING").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //UserGroupInformation.loginUserFromKeytab("t193463@CN.CA", "C:\\Apps\\t193463.keytab")

    import spark.implicits._

    //Reading all the standarised files are creating a dataframe
    val standard_df_schema = StructType(List(
      StructField("runid", StringType),
      StructField("type", StringType),
      StructField("filename", StringType),
      StructField("TimeStamp", LongType)))

    val raw_df_schema = StructType(List(
      StructField("filename", StringType),
      StructField("TimeStamp", LongType)))

    var standard_df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], standard_df_schema)
    val types = FileSystem.get(spark.sparkContext.hadoopConfiguration).
      listStatus(new Path(stan_Path)).map(x => x.getPath.toString)

    types.foreach { type_path =>
      if (FileSystem.get(spark.sparkContext.hadoopConfiguration).
        exists(new Path(type_path + "/process_date=" + process_date + "/"))) {
        val runs = FileSystem.get(spark.sparkContext.hadoopConfiguration).
          listStatus(new Path(type_path + "/process_date=" + process_date + "/")).map(x => x.getPath)
        runs.foreach { run =>
          val data = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(run).map(x =>
            (x.getPath.toString.split("/")(10).split("=").last, x.getPath.toString.split("/")(8),
              x.getPath.toString.split("/").last.replace(".json", ""),
              x.getModificationTime)).toList

          standard_df = standard_df.union(data.toDF())
        }
      }
    }

    var raw_df: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], raw_df_schema)
    val runids = standard_df.select($"runid").distinct().collect()

    runids.foreach{ run_path =>
      if (FileSystem.get(spark.sparkContext.hadoopConfiguration).
        exists(new Path(raw_path + run_path.get(0)))) {
        val types = FileSystem.get(spark.sparkContext.hadoopConfiguration).
          listStatus(new Path(raw_path + run_path.get(0) + "/")).map(x => x.getPath)
        types.foreach { binary =>
          val data = FileSystem.get(spark.sparkContext.hadoopConfiguration).listStatus(binary).map(x =>
            (x.getPath.toString.split("/").last.split("_")(0),x.getModificationTime)).toList

          raw_df = raw_df.union(data.toDF())
        }
      }
    }

    //The fun start now :
    standard_df.createOrReplaceTempView("stand")
    raw_df.createOrReplaceTempView("raw")

    //all data
    spark.sql(
      "select " +
        "runid," +
        "type," +
        "b.TimeStamp - a.TimeStamp as diff " +
        "from stand a " +
        "left join raw b on a.filename = b.filename ").createOrReplaceTempView("joindata")

    //getting avg processing time per type
    spark.sql("select type,avg(diff)/1000 as Diff_sec , count(*) from joindata group by type")
      .coalesce(1)
      .write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite").csv("/user/t193463/prof_testing/per_type/" + process_date )

    //getting avg processing time per run
    spark.sql("select runid,type,avg(diff)/1000 as Processing_time_sec,min(diff)/1000 as Min_processing_time_sec,max(diff)/1000 as Max_processing_time_Sec , count(*) from joindata group by runid,type").
      coalesce(1)
      .write
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite").csv("/user/t193463/prof_testing/per_runid/" + process_date )
  }
}
