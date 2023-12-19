package org.sunbird.obsrv.dataproducts

import com.redislabs.provider.redis._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.native.JsonMethods._
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.dataproducts.helper.BaseMetricHelper
import org.sunbird.obsrv.dataproducts.model.{Edata, MetricLabel}
import org.sunbird.obsrv.dataproducts.util.RestUtil
import org.sunbird.obsrv.model.DatasetModels.{DataSource, Dataset}
import org.sunbird.obsrv.model.DatasetStatus
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable

object MasterDataProcessorIndexer {
  val logger: Logger = LogManager.getLogger(MasterDataProcessorIndexer.getClass)
  val dayPeriodFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC()
  private val restUtil = RestUtil()
  val pwd = System.getProperty("user.dir")
  case class Paths(datasourceRef: String, ingestionPath: String, outputFilePath: String, timestamp: Long)

  case class BlobProvider(sparkProviderURIFormat: String, druidProvider: String, druidProviderPrefix: String)

  def getSparkSession(dataset: Dataset): (SparkSession, SparkContext) = {
    try {
      val conf = new SparkConf()
        .setAppName("MasterDataProcessorIndexer")
        .setMaster("local[*]")
        .set("spark.redis.host", dataset.datasetConfig.redisDBHost.get)
        .set("spark.redis.port", String.valueOf(dataset.datasetConfig.redisDBPort.get))
        .set("spark.redis.db", String.valueOf(dataset.datasetConfig.redisDB.get))
      val spark = SparkSession.builder().config(conf).getOrCreate()
      (spark, spark.sparkContext)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  def indexDataset(config: Config, dataset: Dataset, metrics: BaseMetricHelper, time: Long): Unit = {
    try {
      println("Dataset id: "+dataset.id)
      val datasources = DatasetRegistry.getDatasources(dataset.id)
      val filteredDatasource = datasources.get.filter( datasource =>
        !datasource.metadata.get.aggregated
      )
      if (filteredDatasource.isEmpty || filteredDatasource.size > 1) {
        metrics.generate(ets = new DateTime(DateTimeZone.UTC).getMillis, datasetId = dataset.id, edata = Edata(metric = Map(metrics.getMetricName("failure_dataset_count") -> 1), labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloudStorage.provider")}")), err = "Failed to index dataset.", errMsg = "Dataset should have single datasource."))
        return
      }
      val datasource = filteredDatasource.head
      val paths = getPaths(datasource, config)
      val (spark, sc) = getSparkSession(dataset)
      val events_count = createDataFile(dataset, paths.outputFilePath, sc, spark, config)
      val ingestionSpec = updateIngestionSpec(datasource, paths.datasourceRef, paths.ingestionPath, config)
      submitIngestionTask(ingestionSpec, config)
      DatasetRegistry.updateDatasourceRef(datasource, paths.datasourceRef)
      if (!datasource.datasourceRef.equals(paths.datasourceRef)) {
        deleteDataSource(datasource.datasourceRef, config)
      }
      val end_time = System.currentTimeMillis()
      val success_time = end_time - time
      metrics.generate(ets = new DateTime(DateTimeZone.UTC).getMillis, datasetId = dataset.id, edata = Edata(metric = Map(metrics.getMetricName("success_dataset_count") -> 1, metrics.getMetricName("total_time_taken") -> success_time, metrics.getMetricName("total_events_processed") -> events_count), labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloudStorage.provider")}"))))
    } catch {
      case e: Exception =>
        metrics.generate(ets = new DateTime(DateTimeZone.UTC).getMillis, datasetId = dataset.id, edata = Edata(metric = Map(metrics.getMetricName("failure_dataset_count") -> 1), labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloudStorage.provider")}")), err = "Failed to index dataset.", errMsg = e.getMessage))
        e.printStackTrace()
    }
  }

  def getPaths(datasource: DataSource, config: Config): Paths = {
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

  def updateIngestionSpec(datasource: DataSource, datasourceRef: String, filePath: String, config: Config): String = {
    val deltaIngestionSpec = deltaIngestionSpecProvider(datasourceRef)
    val inputSourceSpec = inputSourceSpecProvider(filePath, config)
    val deltaJson = parse(deltaIngestionSpec)
    val inputSourceJson = parse(inputSourceSpec)
    val ingestionSpec = parse(datasource.ingestionSpec)
    val modIngestionSpec = ingestionSpec merge deltaJson merge inputSourceJson
    compact(render(modIngestionSpec))
  }

  def deltaIngestionSpecProvider(datasourceRef: String): String = {
    val deltaIngestionSpec = s"""{"type":"index_parallel","spec":{"dataSchema":{"dataSource":"$datasourceRef"},"ioConfig":{"type":"index_parallel"},"tuningConfig":{"type":"index_parallel","maxRowsInMemory":500000,"forceExtendableShardSpecs":false,"logParseExceptions":true}}}"""
    deltaIngestionSpec
  }

  def inputSourceSpecProvider(filePath: String, config: Config): String = {
    logger.info("File path -> " + filePath)
    val provider = providerFormat(config.getString("cloudStorage.provider"))
    val inputSourceSpec = if (provider.druidProvider == "local")
      s"""{"spec":{"ioConfig":{"type":"index_parallel","inputSource":{"type":"${provider.druidProvider}","baseDir":"$filePath","filter":"**.json.gz"}}}}"""
    else s"""{"spec":{"ioConfig":{"type":"index_parallel","inputSource":{"type":"${provider.druidProvider}","objectGlob":"**.json.gz","prefixes":["$filePath"]}}}}"""
    inputSourceSpec
  }

  def providerFormat(cloudProvider: String): BlobProvider = {
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
  @throws[Exception]
  def submitIngestionTask(ingestionSpec: String, config: Config) = {
    restUtil.post(config.getString("druid.indexer.url"), ingestionSpec, None)
  }
  @throws[Exception]
  def deleteDataSource(datasourceRef: String, config: Config): Unit = {
    restUtil.delete(config.getString("druid.datasource.delete.url") + datasourceRef, None)
  }

  def createDataFile(dataset: Dataset, outputFilePath: String, sc: SparkContext, spark: SparkSession, config: Config): Long = {
    val readWriteConf = ReadWriteConfig(scanCount = config.getInt("redis_scan_count"), maxPipelineSize = config.getInt("redis_maxPipelineSize"))
    val ts = new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis
    val rdd = sc.fromRedisKV("*")(readWriteConfig = readWriteConf).map(
      f => processEvent(f._2, ts)
    )
    val df = spark.read.json(rdd)
    val noOfRecords = df.count()
    logger.info("Dataset - " + dataset.id + " No. of records - " + noOfRecords)
    df.coalesce(1).write.mode("overwrite").option("compression", "gzip").json(outputFilePath)
    sc.stop()
    spark.stop()
    noOfRecords
  }

  def processEvent(value: String, ts: Long) = {
    val json = JSONUtil.deserialize[mutable.Map[String, AnyRef]](value)
    json("obsrv_meta") = mutable.Map[String, AnyRef]("syncts" -> ts.asInstanceOf[AnyRef]).asInstanceOf[AnyRef]
    JSONUtil.serialize(json)
  }

  // $COVERAGE-OFF$
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("masterdata-indexer.conf").withFallback(ConfigFactory.systemEnvironment())
    val datasets = DatasetRegistry.getAllDatasets("master-dataset")
    val indexedDatasets = datasets.filter(dataset => {
      logger.info("Dataset id - " + dataset.id)
      dataset.datasetConfig.indexData.nonEmpty && dataset.datasetConfig.indexData.get && dataset.status == DatasetStatus.Live
    })
    val metrics = BaseMetricHelper(config)
    indexedDatasets.foreach(dataset => {
      metrics.generate(ets = new DateTime(DateTimeZone.UTC).getMillis, datasetId = dataset.id, edata = Edata(metric = Map(metrics.getMetricName("total_dataset_count") -> 1), labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloudStorage.provider")}"))))
      indexDataset(config, dataset, metrics, System.currentTimeMillis())
    })
  }
  // $COVERAGE-ON$
}