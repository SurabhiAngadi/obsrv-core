package org.sunbird.obsrv.dataproducts

import com.redislabs.provider.redis._
import com.typesafe.config.{Config, ConfigFactory}
import kong.unirest.Unirest
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.native.JsonMethods._
import org.sunbird.obsrv.dataproducts.helper.BaseMetricHelper
import org.sunbird.obsrv.dataproducts.model.{Edata, MetricLabel}
import org.sunbird.obsrv.dataproducts.util.{CommonUtils, StorageUtil}
import org.sunbird.obsrv.model.DatasetModels.{DataSource, Dataset}
import org.sunbird.obsrv.model.DatasetStatus
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable


object MasterDataProcessorIndexer {
  private final val logger: Logger = LogManager.getLogger(MasterDataProcessorIndexer.getClass)

  def indexDataset(config: Config, dataset: Dataset, datasource: DataSource, metrics: BaseMetricHelper, spark: SparkSession, sc: SparkContext) = {
    logger.debug("Indexing dataset ...")
    try {
      val paths = StorageUtil.getPaths(datasource, config)
      val events_count = createDataFile(dataset, paths.outputFilePath, spark, sc, config)
      val ingestionSpec = updateIngestionSpec(datasource, paths.datasourceRef, paths.ingestionPath, config)
      if (events_count > 0L) {
        submitIngestionTask(ingestionSpec, config)
      }
      DatasetRegistry.updateDatasourceRef(datasource, paths.datasourceRef)
      if (!datasource.datasourceRef.equals(paths.datasourceRef)) {
        deleteDataSource(datasource.datasourceRef, config)
      }
      Edata(metric = mutable.Map(metrics.getMetricName("success_dataset_count") -> 1, metrics.getMetricName("total_time_taken") -> 0, metrics.getMetricName("total_events_processed") -> events_count), labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloudStorage.provider")}")))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error("Exception while indexing dataset: " + e.getMessage)
        Edata(metric = mutable.Map(metrics.getMetricName("failure_dataset_count") -> 1), labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloudStorage.provider")}")), err = "Failed to index dataset.", errMsg = e.getMessage)
    }
  }


  def updateIngestionSpec(datasource: DataSource, datasourceRef: String, filePath: String, config: Config): String = {
    val deltaIngestionSpec = config.getString("delta_ingestion_spec").replace("DATASOURCE_REF", datasourceRef)
    val inputSourceSpec = StorageUtil.inputSourceSpecProvider(filePath, config)
    val deltaJson = parse(deltaIngestionSpec)
    val inputSourceJson = parse(inputSourceSpec)
    val ingestionSpec = parse(datasource.ingestionSpec)
    val modIngestionSpec = ingestionSpec merge deltaJson merge inputSourceJson
    compact(render(modIngestionSpec))
  }

  def submitIngestionTask(ingestionSpec: String, config: Config) = {
    logger.debug("Submitting ingestion spec to druid...")
    val response = Unirest.post(config.getString("druid.indexer.url"))
      .header("Content-Type", "application/json")
      .body(ingestionSpec).asJson()
    // $COVERAGE-OFF$
    logger.info(response.getBody)
    response.ifFailure(response => throw new Exception(s"Exception while submitting ingestion task - ${response.getStatus}"))
    // $COVERAGE-ON$
  }

  def deleteDataSource(datasourceRef: String, config: Config): Unit = {
    logger.debug("Deleting datasource...")
    val response = Unirest.delete(config.getString("druid.datasource.delete.url") + datasourceRef)
      .header("Content-Type", "application/json")
      .asJson()
    // $COVERAGE-OFF$
    response.ifFailure(response => throw new Exception("Exception while deleting datasource" + datasourceRef + "with status - " + response.getStatus))
    // $COVERAGE-ON$
  }

  def createDataFile(dataset: Dataset, outputFilePath: String, spark: SparkSession, sc: SparkContext, config: Config) = {
    val readWriteConf = ReadWriteConfig(scanCount = config.getInt("redis_scan_count"), maxPipelineSize = config.getInt("redis_maxPipelineSize"))
    val redisConfig = new RedisConfig(initialHost = RedisEndpoint(host = dataset.datasetConfig.redisDBHost.get, port = dataset.datasetConfig.redisDBPort.get, dbNum = dataset.datasetConfig.redisDB.get))
    val ts = new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis
    val rdd = sc.fromRedisKV("*")(redisConfig = redisConfig, readWriteConfig = readWriteConf).map(
      f => CommonUtils.processEvent(f._2, ts)
    )
    var noOfRecords = 0L
    if (!rdd.isEmpty()) {
      val df = spark.read.json(rdd)
      noOfRecords = df.count()
      logger.info("Dataset - " + dataset.id + " No. of records - " + noOfRecords)
      df.coalesce(20).write.mode("overwrite").option("compression", "gzip").json(outputFilePath)
    }
    noOfRecords
  }

  def processDatasets(config: Config): Unit = {
    val datasets = DatasetRegistry.getAllDatasets("master-dataset")
    val indexedDatasets = datasets.filter(dataset => {
      logger.debug("Checking dataset status for id - " + dataset.id)
      dataset.datasetConfig.indexData.nonEmpty && dataset.datasetConfig.indexData.get && dataset.status == DatasetStatus.Live
    })
    val metrics = BaseMetricHelper(config)
    val conf = new SparkConf()
      .setAppName("MasterDataProcessorIndexer")
      .setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    indexedDatasets.foreach(dataset => {
      logger.info("Starting Index process for dataset - " + dataset.id)
      val datasource: DataSource = CommonUtils.fetchDatasource(dataset)
      if (datasource != null) {
        metrics.generate(datasetId = dataset.id, edata = Edata(metric = mutable.Map(metrics.getMetricName("total_dataset_count") -> 1), labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloudStorage.provider")}"))))
        val (runTime, result) = CommonUtils.getExecutionTime(indexDataset(config, dataset, datasource, metrics, spark, sc))
        result.metric.put(metrics.getMetricName("total_time_taken"), runTime)
        metrics.generate(datasetId = dataset.id, edata = result)
      }
    })
    sc.stop()
    spark.stop()
  }

  // $COVERAGE-OFF$
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("masterdata-indexer.conf").withFallback(ConfigFactory.systemEnvironment())
    processDatasets(config)
  }
  // $COVERAGE-ON$
}