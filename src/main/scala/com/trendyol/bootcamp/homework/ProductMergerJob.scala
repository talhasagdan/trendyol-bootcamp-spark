package com.trendyol.bootcamp.homework
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import scala.util.Try


case class Product(id:Long,name:String,category:String,brand:String,color:String,price:Double,timestamp:Long)

object ProductMergerJob {
  def main(args: Array[String]): Unit = {

    /**
      * Find the latest version of each product in every run, and save it as snapshot.
      *
      * Product data stored under the data/homework folder.
      * Read data/homework/initial_data.json for the first run.
      * Read data/homework/cdc_data.json for the nex runs.
      *
      * Save results as json, parquet or etc.
      *
      * Note: You can use SQL, dataframe or dataset APIs, but type safe implementation is recommended.
      */

    //Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Product Merge Jobs")
      .getOrCreate()

    //Configuration of hadoop file system
    //Using fs varible for move, delete or rename operations
    val config = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(config)

    //defining paths as path type for fs
    val batchPath :Path = new Path("data/batch/output/batch")
    val tempBatchPath :Path =  new Path("data/batch/output/batch_temp")

    import spark.implicits._

    // creating empty dataset for initial batch folder when empty
    val emptyDataset = spark.emptyDataset[Product]

    // reading initial and cdc datasets with reader metot
    val initialDataset = reader("data/homework/initial_data.json",spark,emptyDataset).as[Product]
    val cdc = reader("data/homework/cdc_data.json",spark,emptyDataset).as[Product]

    //using lastVersionReader provides us read batch directory, we don't use reader metot for readle code
    // lastVersionReader : Dataset[Product] then we use input for merger directly
    val firstmerge = merger(lastVersionReader(spark,emptyDataset),initialDataset,spark)
    // writer metot inside batch our merged data
    writer(firstmerge,spark,"data/batch/output/batch")

    // same operation for merger and lastVersionReader
    val secondmerge = merger(lastVersionReader(spark,emptyDataset),cdc,spark)

    //creation of batch_temp directory
    fileCreate(spark,fs,tempBatchPath)

    //we write batch_temp new data then delete batch and rename it => batch
    writer(secondmerge,spark,"data/batch/output/batch_temp")
    fileDelete(spark,fs,batchPath,tempBatchPath)

  }

  def reader( path:String , spark:SparkSession, emptyDataset:Dataset[Product])={
    //reading any directory : Dataset[Product]
    import spark.implicits._
    val productsSchema = Encoders.product[Product].schema

    spark.read
      .schema(productsSchema)
      .json(path).as[Product]
  }
  def lastVersionReader(spark: SparkSession, emptyDataset:Dataset[Product])= {
    //reading batch directory : Dataset[Product]
    import spark.implicits._
    val productsSchema = Encoders.product[Product].schema

    Try(spark.read
      .schema(productsSchema)
      .json("data/batch/output/batch").as[Product])
      .getOrElse(emptyDataset)

  }

  def merger (lastVersionOfDataset : Dataset[Product],cdcDataset:Dataset[Product],spark: SparkSession)={
    //union two datasets then eleminate based timestamp
    import spark.implicits._
    val unionOfTwoDataset = lastVersionOfDataset.union(cdcDataset)
    val elemination = unionOfTwoDataset
      .groupByKey(updateProducts => updateProducts.id )
      .reduceGroups{(last,cdc) =>
        if (last.timestamp>cdc.timestamp) last
        else cdc
      }.map{case(id,value)=> value}
    elemination
  }

  def writer(updatedDataset:Dataset[Product],spark:SparkSession,path:String)={
    //write our data
      updatedDataset
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(path)
  }

  def fileCreate(spark:SparkSession,fs:FileSystem,tempBatchPath:Path)={
    //creating file
    fs.mkdirs(tempBatchPath)
  }
  def fileDelete(sparkSession: SparkSession,fs:FileSystem,batchPath: Path,tempBatchPath: Path): Unit ={
    // delete and renaming files
    fs.delete(batchPath,true)
    fs.rename(tempBatchPath,batchPath)
  }

}

