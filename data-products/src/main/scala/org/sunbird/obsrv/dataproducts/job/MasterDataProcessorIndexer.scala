package org.sunbird.obsrv.dataproducts.job

import com.redislabs.provider.redis._
import com.typesafe.config.{Config, ConfigFactory}
import kong.unirest.Unirest
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.native.JsonMethods._
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.dataproducts.helper.{BaseMetricHelper, KafkaMessageProducer}
import org.sunbird.obsrv.dataproducts.model.{Edata, MetricLabel}
import org.sunbird.obsrv.model.DatasetModels.{DataSource, Dataset}
import org.sunbird.obsrv.registry.DatasetRegistry
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.spark.sql.functions._

import scala.collection.mutable

object MasterDataProcessorIndexer {
  private final val logger: Logger = LogManager.getLogger(MasterDataProcessorIndexer.getClass)
  private val config: Config = ConfigFactory.load("masterdata-indexer.conf").withFallback(ConfigFactory.systemEnvironment())
  private val dayPeriodFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC()

  case class Paths(datasourceRef: String, ingestionPath: String, outputFilePath: String, timestamp: Long)

  case class BlobProvider(sparkProviderURIFormat: String, druidProvider: String, druidProviderPrefix: String)

  def main(args: Array[String]): Unit = {
    val datasets = DatasetRegistry.getAllDatasets("master-dataset")
    val indexedDatasets = datasets.filter(dataset => {
      logger.info("Dataset id - " + dataset.id)
      dataset.datasetConfig.indexData.nonEmpty && dataset.datasetConfig.indexData.get
    })
    val metrics = BaseMetricHelper(config)
    indexedDatasets.foreach(dataset => {
      metrics.generate(ets = new DateTime(DateTimeZone.UTC).getMillis, datasetId = dataset.id, edata = Edata(metric = Map(metrics.getMetricName("total_dataset_count") -> 1), labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloudStorage.provider")}"))))
      indexDataset(dataset, metrics, System.currentTimeMillis())
    })
  }

  def indexDataset(dataset: Dataset, metrics: BaseMetricHelper, time: Long): Unit = {
    try {
      val datasources = DatasetRegistry.getDatasources(dataset.id)
      if (datasources.isEmpty || datasources.get.size > 1) {
        return
      }
      val datasource = datasources.get.head
      val paths = getPaths(datasource)
      val conf = new SparkConf()
        .setAppName("MasterDataProcessorIndexer")
        .set("spark.redis.host", dataset.datasetConfig.redisDBHost.get)
        .set("spark.redis.port", String.valueOf(dataset.datasetConfig.redisDBPort.get))
        .set("spark.redis.db", String.valueOf(dataset.datasetConfig.redisDB.get))
      val spark = new SparkSession.Builder().config(conf).getOrCreate()
      val sc = spark.sparkContext
      val events_count = createDataFile(dataset, paths.outputFilePath, sc,  spark)
//      val ingestionSpec = updateIngestionSpec(datasource, paths.datasourceRef, paths.ingestionPath)
//      submitIngestionTask(ingestionSpec)
//      DatasetRegistry.updateDatasourceRef(datasource, paths.datasourceRef)
//      if (!datasource.datasourceRef.equals(paths.datasourceRef)) {
//        deleteDataSource(datasource.datasourceRef)
//      }
//      val end_time = System.currentTimeMillis()
//      val success_time = end_time - time
//      metrics.generate(ets = new DateTime(DateTimeZone.UTC).getMillis, datasetId = dataset.id, edata = Edata(metric = Map(metrics.getMetricName("success_dataset_count") -> 1, metrics.getMetricName("total_time_taken") -> success_time, metrics.getMetricName("total_events_processed") -> events_count), labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloudStorage.provider")}"))))
    } catch {
      case e: Exception =>
        metrics.generate(ets = new DateTime(DateTimeZone.UTC).getMillis, datasetId = dataset.id, edata = Edata(metric = Map(metrics.getMetricName("failure_dataset_count") -> 1), labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloudStorage.provider")}")), err = "Failed to index dataset.", errMsg = e.getMessage))
        e.printStackTrace()
    }
  }


  def getPaths(datasource: DataSource): Paths = {
    val dt = new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay()
    val timestamp = dt.getMillis
    val date = dayPeriodFormat.print(dt)
    val provider = providerFormat(config.getString("cloudStorage.provider"))
    val cloudPrefix = if (config.getString("cloudStorage.provider") == "azure") {
      provider.sparkProviderURIFormat + s"""://${config.getString("cloudStorage.accountName")}.blob.core.windows.net/${config.getString("cloudStorage.container")}/"""
    } else {
      provider.sparkProviderURIFormat + s"""://${config.getString("cloudStorage.container")}/"""
    }
    val pathSuffix = s"""masterdata-indexer/${datasource.datasetId}/$date/"""
    val ingestionPath = cloudPrefix.replace(provider.sparkProviderURIFormat, provider.druidProviderPrefix) + pathSuffix
    val datasourceRef = datasource.datasource + '-' + date
    val outputFilePath = cloudPrefix + pathSuffix
    Paths(datasourceRef, ingestionPath, outputFilePath, timestamp)
  }


  private def updateIngestionSpec(datasource: DataSource, datasourceRef: String, filePath: String): String = {
    val deltaIngestionSpec = deltaIngestionSpecProvider(datasourceRef)
    val inputSourceSpec = inputSourceSpecProvider(filePath)
    val deltaJson = parse(deltaIngestionSpec)
    val inputSourceJson = parse(inputSourceSpec)
    val ingestionSpec = parse(datasource.ingestionSpec)
    val modIngestionSpec = ingestionSpec merge deltaJson merge inputSourceJson
    compact(render(modIngestionSpec))
  }

  private def deltaIngestionSpecProvider(datasourceRef: String): String = {
    val deltaIngestionSpec = s"""{"type":"index_parallel","spec":{"dataSchema":{"dataSource":"$datasourceRef"},"ioConfig":{"type":"index_parallel"},"tuningConfig":{"type":"index_parallel","maxRowsInMemory":500000,"forceExtendableShardSpecs":false,"logParseExceptions":true}}}"""
    deltaIngestionSpec
  }

  private def inputSourceSpecProvider(filePath: String): String = {
    val provider = providerFormat(config.getString("cloudStorage.provider"))
    val inputSourceSpec = if (provider.druidProvider == "local")
      s"""{"spec":{"ioConfig":{"type":"index_parallel","inputSource":{"type":"${provider.druidProvider}","baseDir":"${config.getString("cloudStorage.container")}","filter":"**.json.gz"}}}}"""
    else s"""{"spec":{"ioConfig":{"type":"index_parallel","inputSource":{"type":"${provider.druidProvider}","objectGlob":"**.json.gz","prefixes":["$filePath"]}}}}"""
    inputSourceSpec
  }

  private def providerFormat(cloudProvider: String): BlobProvider = {
    cloudProvider match {
      case "local" => BlobProvider("file", "local", "file")
      case "aws" => BlobProvider("s3a", "s3", "s3")
      case "azure" => BlobProvider("wasbs", "azure", "azure")
      case "gcloud" => BlobProvider("gs", "google", "gs")
      case "cephs3" => BlobProvider("s3a", "s3", "s3") // TODO: Have to check Druid compatibility
      case "oci" => BlobProvider("s3a", "s3", "s3") // TODO: Have to check Druid compatibility
      case _ => throw new Exception("Unsupported provider")
    }
  }

  private def submitIngestionTask(ingestionSpec: String) = {
    val response = Unirest.post(config.getString("druid.indexer.url"))
      .header("Content-Type", "application/json")
      .body(ingestionSpec).asJson()
    logger.info("Ingestion spec - " + response.getBody)
    response.ifFailure(response => throw new Exception(s"Exception while submitting ingestion task - ${response.getBody}"))
  }

  private def deleteDataSource(datasourceRef: String): Unit = {
    val response = Unirest.delete(config.getString("druid.datasource.delete.url") + datasourceRef)
      .header("Content-Type", "application/json")
      .asJson()
    response.ifFailure(response => throw new Exception("Exception while deleting datasource" + datasourceRef + " ,Response body - " + response.getBody + "Status - " + response.getStatus))
  }

  def createDataFile(dataset: Dataset, outputFilePath: String, sc: SparkContext, spark: SparkSession ) = {
    import spark.implicits._
    val readWriteConf = ReadWriteConfig(scanCount = config.getInt("redis_scan_count"), maxPipelineSize = config.getInt("redis_maxPipelineSize"))
    val df = sc.fromRedisKV("*")(readWriteConfig = readWriteConf)
      .map(f =>
        processEvent(f._2)
      ).toDF()
    // Show the result
    //      .map(f =>
    //        processEvent(f._2)
    //      ).toDF()
    //    val noOfRecords = df.count()
    //    val response = rdd.collect()
    //    val stringifiedResponse = JSONUtil.serialize(response)
    //    val df = spark.read.json(spark.sparkContext.parallelize(event))
    val noOfRecords = df.count()
    logger.info("Dataset - " + dataset.id + " No. of records - " + noOfRecords)
    df.coalesce(1).write.mode("overwrite").option("compression", "gzip").json(outputFilePath)
    sc.stop()
    spark.stop()
    noOfRecords
  }

  private def processEvent(value: String) = {
    val dt = new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay()
    val timestamp = dt.getMillis
    val json = JSONUtil.deserialize[mutable.Map[String, AnyRef]](value)
    json("obsrv_meta") = mutable.Map[String, AnyRef]("syncts" -> timestamp.asInstanceOf[AnyRef]).asInstanceOf[AnyRef]
    JSONUtil.serialize(json)
  }
}