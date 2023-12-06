package org.sunbird.obsrv.spec

import com.redislabs.provider.redis._
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import kafka.log.LogManager
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.mockito.Mockito.{times, verify, when}
import org.mockito.MockitoSugar.mock
import org.sunbird.obsrv.registry.DatasetRegistry
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, path}
import org.sunbird.fixture.EventFixture
import org.sunbird.obsrv.core.cache.RedisConnect
import org.sunbird.obsrv.core.util.{JSONUtil, PostgresConnect, PostgresConnectionConfig}
import org.sunbird.obsrv.dataproducts.helper.{BaseMetricHelper, KafkaMessageProducer}
import org.sunbird.obsrv.dataproducts.job.MasterDataProcessorIndexer
import org.sunbird.obsrv.dataproducts.model.{Actor, Context, Edata, JobMetric, MetricLabel, MetricObject, Pdata}
import org.sunbird.obsrv.model.DatasetModels.DataSource
import redis.embedded.RedisServer

import java.util.UUID
import okhttp3.mockwebserver.{MockResponse, MockWebServer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.spi.LoggerFactory
import org.apache.logging.log4j.Logger
import org.mockito.ArgumentMatchers.any

class MasterDataIndexerSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  private val jobConfig: Config = ConfigFactory.load("masterdata-indexer.conf").withFallback(ConfigFactory.systemEnvironment())
  private val azureConfig: Config = ConfigFactory.load("azure-cred.conf").withFallback(ConfigFactory.systemEnvironment())

  val metrics = BaseMetricHelper(jobConfig)

  val config: Config = ConfigFactory.load("test.conf")
  val postgresConfig = PostgresConnectionConfig(
    user = config.getString("postgres.user"),
    password = config.getString("postgres.password"),
    database = "postgres",
    host = config.getString("postgres.host"),
    port = config.getInt("postgres.port"),
    maxConnections = config.getInt("postgres.maxConnections")
  )

  var embeddedPostgres: EmbeddedPostgres = _
  var redisServer: RedisServer = _
  var redisConnection: RedisConnect = _
  var mockServer = new MockWebServer()
  private val dayPeriodFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC()
  val dt = new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay()
  val date = dayPeriodFormat.print(dt)

  val customKafkaConsumerProperties: Map[String, String] = Map[String, String]("auto.offset.reset" -> "earliest", "group.id" -> "test-event-schema-group")
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort = 9093,
      zooKeeperPort = 2183,
      customConsumerProperties = customKafkaConsumerProperties
    )
  implicit val deserializer: StringDeserializer = new StringDeserializer()


  override def beforeAll(): Unit = {
    super.beforeAll()
    redisServer = new RedisServer(6340)
    redisServer.start()
    embeddedPostgres = EmbeddedPostgres.builder.setPort(5432).start()
    val postgresConnect = new PostgresConnect(postgresConfig)
    createSchema(postgresConnect)
    insertTestData(postgresConnect)
    redisConnection = new RedisConnect("localhost", 6340, 30000)
    val jedis = redisConnection.getConnection(3)
    jedis.set("device-00", EventFixture.d1)
    jedis.set("device-01", EventFixture.d2)
    jedis.set("device-02", EventFixture.d3)
    jedis.set("device-03", EventFixture.d4)
    jedis.set("device-04", EventFixture.d5)
    EmbeddedKafka.start()(embeddedKafkaConfig)
    createTestTopics()
    mockServer.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    redisServer.stop()
    mockServer.shutdown()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    EmbeddedKafka.createCustomTopic("spark.stats")
  }

  private def createSchema(postgresConnect: PostgresConnect) {
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS datasets ( id text PRIMARY KEY, type text NOT NULL, validation_config json, extraction_config json, dedup_config json, data_schema json, denorm_config json, router_config json NOT NULL, dataset_config json NOT NULL, status text NOT NULL, tags text[], data_version INT, created_by text NOT NULL, updated_by text NOT NULL, created_date timestamp NOT NULL, updated_date timestamp NOT NULL );")
  }

  private def insertTestData(postgresConnect: PostgresConnect) = {
    postgresConnect.execute("insert into datasets(id, type, validation_config, extraction_config, dedup_config, data_schema, denorm_config, router_config, dataset_config, tags, data_version, status, created_by, updated_by, created_date, updated_date) VALUES('md1','master-dataset', '{\"validate\": true, \"mode\": \"Strict\", \"validation_mode\": {}}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 1036800}, \"batch_id\": \"id\"}', '{\"drop_duplicates\": true, \"dedup_key\": \"device_id\", \"dedup_period\": 1036800}', '{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": {\"fcm_token\": {\"type\": \"string\"}, \"city\": {\"type\": \"string\"}, \"device_id\": {\"type\": \"string\"}, \"device_spec\": {\"type\": \"string\"}, \"state\": {\"type\": \"string\"}, \"uaspec\": {\"type\": \"object\", \"properties\": {\"agent\": {\"type\": \"string\"}, \"ver\": {\"type\": \"string\"}, \"system\": {\"type\": \"string\"}, \"raw\": {\"type\": \"string\"}}}, \"country\": {\"type\": \"string\"}, \"country_code\": {\"type\": \"string\"}, \"producer_id\": {\"type\": \"string\"}, \"state_code_custom\": {\"type\": \"integer\"}, \"state_code\": {\"type\": \"string\"}, \"state_custom\": {\"type\": \"string\"}, \"district_custom\": {\"type\": \"string\"}, \"first_access\": {\"type\": \"integer\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''first_access'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\"}]}, \"api_last_updated_on\": {\"type\": \"integer\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''api_last_updated_on'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\"}]}, \"user_declared_district\": {\"type\": \"string\"}, \"user_declared_state\": {\"type\": \"string\"}}, \"required\": [\"first_access\", \"api_last_updated_on\", \"device_id\"]}', '{\"redis_db_host\": \"localhost\", \"redis_db_port\": 6340, \"denorm_fields\": []}', '{\"topic\": \",d1\"}', '{\"data_key\": \"device_id\", \"timestamp_key\": \"\", \"exclude_fields\": [], \"entry_topic\": \"local.masterdata.ingest\", \"redis_db_host\": \"localhost\", \"redis_db_port\": 6340, \"index_data\": true, \"redis_db\": 3}', NULL, NULL, 'ACTIVE', 'SYSTEM', 'SYSTEM', '2023-10-04 06:44:11.600', '2023-10-04 06:44:11.600');")
    postgresConnect.execute("insert into datasets(id, type, validation_config, extraction_config, dedup_config, data_schema, denorm_config, router_config, dataset_config, tags, data_version, status, created_by, updated_by, created_date, updated_date) VALUES('md2','master-dataset', '{\"validate\": true, \"mode\": \"Strict\", \"validation_mode\": {}}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 1036800}, \"batch_id\": \"id\"}', '{\"drop_duplicates\": true, \"dedup_key\": \"device_id\", \"dedup_period\": 1036800}', '{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": {\"fcm_token\": {\"type\": \"string\"}, \"city\": {\"type\": \"string\"}, \"device_id\": {\"type\": \"string\"}, \"device_spec\": {\"type\": \"string\"}, \"state\": {\"type\": \"string\"}, \"uaspec\": {\"type\": \"object\", \"properties\": {\"agent\": {\"type\": \"string\"}, \"ver\": {\"type\": \"string\"}, \"system\": {\"type\": \"string\"}, \"raw\": {\"type\": \"string\"}}}, \"country\": {\"type\": \"string\"}, \"country_code\": {\"type\": \"string\"}, \"producer_id\": {\"type\": \"string\"}, \"state_code_custom\": {\"type\": \"integer\"}, \"state_code\": {\"type\": \"string\"}, \"state_custom\": {\"type\": \"string\"}, \"district_custom\": {\"type\": \"string\"}, \"first_access\": {\"type\": \"integer\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''first_access'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\"}]}, \"api_last_updated_on\": {\"type\": \"integer\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''api_last_updated_on'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\"}]}, \"user_declared_district\": {\"type\": \"string\"}, \"user_declared_state\": {\"type\": \"string\"}}, \"required\": [\"first_access\", \"api_last_updated_on\", \"device_id\"]}', '{\"redis_db_host\": \"localhost\", \"redis_db_port\": 6340, \"denorm_fields\": []}', '{\"topic\": \",d1\"}', '{\"data_key\": \"device_id\", \"timestamp_key\": \"\", \"exclude_fields\": [], \"entry_topic\": \"local.masterdata.ingest\", \"redis_db_host\": \"\", \"redis_db_port\": \"\", \"index_data\": true, \"redis_db\": \"\"}', NULL, NULL, 'ACTIVE', 'SYSTEM', 'SYSTEM', 'now()', 'now()');")
  }

  "indexDataset method" should "index datasets for single datasource and generate metrics" in {
    val dataset = DatasetRegistry.getDataset("md1")
    val datasource = DataSource("datasource1", "md1", "", s"datasource1-${date}")
    datasource.datasource.isEmpty should be(false)
    MasterDataProcessorIndexer.indexDataset(jobConfig, dataset.get, metrics, 0L)
  }

  "indexDataset method" should "not index datasets for empty datasource and generate metrics" in {
    val dataset = DatasetRegistry.getAllDatasets("master-dataset")
    val datasource = DatasetRegistry.getDatasources("md2")
    datasource.isEmpty should be(true)
    MasterDataProcessorIndexer.indexDataset(jobConfig, dataset.head, metrics, 0L)
  }

  "The getPaths function" should "return the correct paths for any cloud storage" in {
    val datasource = DataSource("datasource1", "md1", "", s"datasource1-${date}")
    val paths = MasterDataProcessorIndexer.getPaths(datasource, jobConfig)
    val provider = MasterDataProcessorIndexer.providerFormat(jobConfig.getString("cloudStorage.provider"))
    paths.datasourceRef shouldEqual s"datasource1-${date}"
    paths.ingestionPath shouldEqual s"${provider.sparkProviderURIFormat}://${jobConfig.getString("cloudStorage.container")}/masterdata-indexer/${datasource.datasetId}/${date}/"
    paths.outputFilePath shouldEqual s"${provider.sparkProviderURIFormat}://${jobConfig.getString("cloudStorage.container")}/masterdata-indexer/${datasource.datasetId}/${date}/"
  }

  "The getPaths function" should "return the correct paths for azure cloud storage" in {
    val datasource = DataSource("datasource1", "md1", "", s"datasource1-${date}")
    val paths = MasterDataProcessorIndexer.getPaths(datasource, azureConfig)
    paths.datasourceRef shouldEqual s"datasource1-${date}"
    paths.ingestionPath shouldEqual s"azure://obsrv-data/masterdata-indexer/md1/${date}/"
    paths.outputFilePath shouldEqual s"wasbs://obsrv-data/masterdata-indexer/md1/${date}/"
  }

  "The updateIngestionSpec function" should "update the ingestion spec with the correct data source reference and file path" in {
    val mockDatasource = DataSource("datasource1", "md1", "", s"datasource1-${date}")
    val mockDatasourceRef = s"datasource1-${date}"
    val mockFilepath = s"file:///home/sankethika/obsrv-data/masterdata-indexer/datasource1/${date}/"
    val ingestionSpec = s"""{"type":"index_parallel","spec":{"dataSchema":{"dataSource":"datasource1-${date}"},"ioConfig":{"type":"index_parallel","inputSource":{"type":"local","baseDir":"/home/sankethika/obsrv-data","filter":"**.json.gz"}},"tuningConfig":{"type":"index_parallel","maxRowsInMemory":500000,"forceExtendableShardSpecs":false,"logParseExceptions":true}}}"""
    val deltaIngestionSpecProvider = s"""{"type":"index_parallel","spec":{"dataSchema":{"dataSource":"$mockDatasourceRef"},"ioConfig":{"type":"index_parallel"},"tuningConfig":{"type":"index_parallel","maxRowsInMemory":500000,"forceExtendableShardSpecs":false,"logParseExceptions":true}}}"""
    val inputSourceSpec = """{"spec":{"ioConfig":{"type":"index_parallel","inputSource":{"type":"local","baseDir":"/home/sankethika/obsrv-data","filter":"**.json.gz"}}}}"""
    assert(mockDatasource.ingestionSpec == "")

    val deltaIngestionSpec = MasterDataProcessorIndexer.deltaIngestionSpecProvider(mockDatasourceRef)
    assert(deltaIngestionSpec == deltaIngestionSpecProvider)

    val inputSpec = MasterDataProcessorIndexer.inputSourceSpecProvider(mockFilepath, jobConfig)
    assert(inputSpec == inputSourceSpec)

    val updatedSpec = MasterDataProcessorIndexer.updateIngestionSpec(mockDatasource, mockDatasourceRef, mockFilepath, jobConfig)
    assert(updatedSpec == ingestionSpec)
  }

  "deltaIngestionSpecProvider" should "generate appropriate deltaIngestionSpec" in {
    val datasourceRef = s"datasource1-${date}"
    val deltaIngestionSpecProvider = s"""{"type":"index_parallel","spec":{"dataSchema":{"dataSource":"$datasourceRef"},"ioConfig":{"type":"index_parallel"},"tuningConfig":{"type":"index_parallel","maxRowsInMemory":500000,"forceExtendableShardSpecs":false,"logParseExceptions":true}}}"""
    val deltaIngestionSpec = MasterDataProcessorIndexer.deltaIngestionSpecProvider(datasourceRef)
    assert(deltaIngestionSpec == deltaIngestionSpecProvider)
  }

  "inputSourceSpecProvider" should "generate appropriate inputSourceSpec for local storage" in {
    val filePath = s"file:///home/sankethika/masterdata-indexer/md1/20231205/"
    val expectedInputSourceSpec = s"""{"spec":{"ioConfig":{"type":"index_parallel","inputSource":{"type":"local","baseDir":"/home/sankethika/obsrv-data","filter":"**.json.gz"}}}}"""
    val actualInputSourceSpec = MasterDataProcessorIndexer.inputSourceSpecProvider(filePath, jobConfig)
    assert(actualInputSourceSpec == expectedInputSourceSpec)
  }

  "inputSourceSpecProvider" should "generate appropriate inputSourceSpec for different cloud providers" in {
    val provider = jobConfig.withValue("cloudStorage.provider", ConfigValueFactory.fromAnyRef("aws"))
    val filePath = "s3a://obsrv-data/masterdata-indexer/md1/20231205/"
    val expectedInputSourceSpec = s"""{"spec":{"ioConfig":{"type":"index_parallel","inputSource":{"type":"s3","objectGlob":"**.json.gz","prefixes":["s3a://obsrv-data/masterdata-indexer/md1/20231205/"]}}}}"""
    val actualInputSourceSpec = MasterDataProcessorIndexer.inputSourceSpecProvider(filePath, provider)
    assert(actualInputSourceSpec == expectedInputSourceSpec)
  }

  "The Paths case class" should "correctly initialize attributes" in {
    val datasourceRef = s"datasource1-${date}"
    val ingestionPath = "file:///home/sankethika/obsrv-data/masterdata-indexer/datasource1/20231204/"
    val outputFilePath = "file:///home/sankethika/obsrv-data/masterdata-indexer/datasource1/20231204/"
    val timestamp = 1670252800000L

    val paths = MasterDataProcessorIndexer.Paths(datasourceRef, ingestionPath, outputFilePath, timestamp)

    paths.datasourceRef shouldEqual datasourceRef
    paths.ingestionPath shouldEqual ingestionPath
    paths.outputFilePath shouldEqual outputFilePath
    paths.timestamp shouldEqual timestamp
  }

  "MasterDataIndexerProcessor" should "create SparkContext and SparkSession" in {
    val dataset = DatasetRegistry.getDataset("md1")
    dataset.get.datasetConfig.redisDBHost shouldEqual Some("localhost")
    dataset.get.datasetConfig.redisDBPort shouldEqual Some(6340)
    dataset.get.datasetConfig.redisDB shouldEqual Some(3)

    val (spark, sc) = MasterDataProcessorIndexer.getSparkSession(dataset.get)

    spark should not be null
    sc should not be null
  }

  "MasterDataIndexerProcessor" should "throw exception while creating Spark session" in {
    val dataset = DatasetRegistry.getDataset("md2")
    dataset.get.datasetConfig.redisDBHost shouldEqual Some("")
    dataset.get.datasetConfig.redisDBPort shouldEqual Some("")
    dataset.get.datasetConfig.redisDB shouldEqual Some("")

    assertThrows[Exception] {
      MasterDataProcessorIndexer.getSparkSession(dataset.get)
    }
  }

  "MasterDataIndexerProcessor" should "generate correct number of records in output file" in {
    val dataset = DatasetRegistry.getDataset("md1")
    val outputFilePath = "file:///home/sankethika/obsrv-data/masterdata-indexer/datasource1/20231205/"
    val (spark, sc) = MasterDataProcessorIndexer.getSparkSession(dataset.get)
    val jedis = redisConnection.getConnection(3)
    val noOfRecords = MasterDataProcessorIndexer.createDataFile(dataset.get, outputFilePath, sc, spark, jobConfig)
    noOfRecords should be(jedis.dbSize())
  }

  "ProviderFormat" should "return the correct BlobProvider for known cloud providers" in {
    MasterDataProcessorIndexer.providerFormat("local") shouldEqual MasterDataProcessorIndexer.BlobProvider("file", "local", "file")
    MasterDataProcessorIndexer.providerFormat("aws") shouldEqual MasterDataProcessorIndexer.BlobProvider("s3a", "s3", "s3")
    MasterDataProcessorIndexer.providerFormat("azure") shouldEqual MasterDataProcessorIndexer.BlobProvider("wasbs", "azure", "azure")
    MasterDataProcessorIndexer.providerFormat("gcloud") shouldEqual MasterDataProcessorIndexer.BlobProvider("gs", "google", "gs")
    MasterDataProcessorIndexer.providerFormat("cephs3") shouldEqual MasterDataProcessorIndexer.BlobProvider("s3a", "s3", "s3")
    MasterDataProcessorIndexer.providerFormat("oci") shouldEqual MasterDataProcessorIndexer.BlobProvider("s3a", "s3", "s3")
    an[Exception] should be thrownBy MasterDataProcessorIndexer.providerFormat("unknownProvider")
  }


  "MasterDataIndexerProcessor" should "index dataset successfully for single data source" in {
    val dataset = DatasetRegistry.getDataset("md1")
    val filePath = s"file:///home/sankethika/obsrv-data/masterdata-indexer/datasource1/${date}/"
    val (spark, sc) = MasterDataProcessorIndexer.getSparkSession(dataset.get)
    MasterDataProcessorIndexer.indexDataset(jobConfig, dataset.get, metrics, 0L)
    val noOfRecords = MasterDataProcessorIndexer.createDataFile(dataset.get, filePath, sc, spark, jobConfig)
    noOfRecords should be(5)
  }

  "MasterDataIndexerProcessor" should "submit ingestion task successfully" in {
    mockServer.enqueue(new MockResponse().setResponseCode(200).setBody("Mock Response"))
    val testConfig = jobConfig.withValue("druid.indexer.url", ConfigValueFactory.fromAnyRef(mockServer.url("http://localhost:8888/druid/indexer/v1/task").toString))
    val ingestionSpec = s"""{"type":"index_parallel","spec":{"dataSchema":{"dataSource":"datasource1-${date}"},"ioConfig":{"type":"index_parallel","inputSource":{"type":"local","baseDir":"/home/sankethika/obsrv-data","filter":"**.json.gz"}},"tuningConfig":{"type":"index_parallel","maxRowsInMemory":500000,"forceExtendableShardSpecs":false,"logParseExceptions":true}}}"""
    MasterDataProcessorIndexer.submitIngestionTask(ingestionSpec, testConfig)
    val request = mockServer.takeRequest()
    request.getMethod shouldBe "POST"
    request.getHeader("Content-Type") shouldBe "application/json"
    request.getBody.readUtf8() shouldBe ingestionSpec
  }

  "JobMetric" should "create actor form valid parameters" in {
    val id = "JobId"
    val type_ = "SYSTEM"
    val actor = Actor(id, type_)
    assert(actor.id == id)
    assert(actor.`type` == type_)
  }

  "JobMetric" should "create context from valid parameters" in {
    val env = "local"
    val pdata = Pdata(id = "DataProducts", pid = "MasterDataProcessorIndexerJob", ver = "1.0.0")
    val context = Context(env, pdata)
    assert(context.env == env)
    assert(context.pdata == pdata)
  }

  "JobMetric" should "create Edata with valid parameters" in {
    val metric = Map("key" -> "value")
    val labels = Seq(MetricLabel("label-key", "label-value"))
    val edata = Edata(metric, labels)

    assert(edata.metric === metric)
    assert(edata.labels === labels)
    assert(edata.err === null)
    assert(edata.errMsg === null)
  }

  "JobMetric" should "handle error fields" in {
    val metric = Map("key" -> "value")
    val labels = Seq(MetricLabel("label-key", "label-value"))
    val err = "Some error message"
    val errMsg = "Exception details"
    val edata = Edata(metric, labels, err, errMsg)

    assert(edata.err === err)
    assert(edata.errMsg === errMsg)
  }

  "JobMetric" should "create MetricLabel from valid parameters" in {
    val key = "metric-key"
    val value = "metric-value"
    val label = MetricLabel(key, value)

    assert(label.key == key)
    assert(label.value == value)
  }

  "JobMetric" should "create MetricObject with valid parameters" in {
    val id = "object-id"
    val type_ = "object-type"
    val ver = "object-ver"
    val metricObject = MetricObject(id, type_, ver)

    assert(metricObject.id === id)
    assert(metricObject.`type` === type_)
    assert(metricObject.ver === ver)
  }

  "JobMetric" should "create metric from valid parameters" in {
    val eid = "Metric"
    val ets = System.currentTimeMillis()
    val mid = UUID.randomUUID().toString
    val actor = Actor("id", "SYSTEM")
    val env = "dev"
    val pdata = Pdata("pdata-id", "pid-1", "v1")
    val context = Context(env, pdata)
    val metricObject = MetricObject("object-id", "object-type", "object-ver")
    val edata = Edata(Map("key" -> "value"), Seq(MetricLabel("label-key", "label-value")))
    val jobMetric = JobMetric(eid, ets, mid, actor, context, metricObject, edata)

    assert(jobMetric.eid == eid)
    assert(jobMetric.ets == ets)
    assert(jobMetric.mid == mid)
    assert(jobMetric.actor == actor)
    assert(jobMetric.context == context)
    assert(jobMetric.`object` == metricObject)
    assert(jobMetric.edata == edata)
  }

  "BaseMetricsHelper" should "generate metric name" in {
    val metricHelper = BaseMetricHelper(jobConfig)
    val metricName = metricHelper.getMetricName("total_dataset_count")
    metricName shouldBe "total_dataset_count"
  }

  "BaseMetricsHelper" should "generate metric object" in {
    val dataset = DatasetRegistry.getDataset("md1")
    val metricHelper = BaseMetricHelper(jobConfig)
    val metricObject = metricHelper.getObject(dataset.get.id)
    metricObject shouldBe MetricObject(id = dataset.get.id, `type` = "Dataset", ver = "1.0.0")
  }

  "BaseMetricHelper" should "generate metric" in {
    val dataset = DatasetRegistry.getDataset("md1")
    val edata = Edata(Map("key" -> "value"), Seq(MetricLabel("label-key", "label-value")))

    val mockMetricsProducer = mock[KafkaMessageProducer]
    val baseMetricHelperWithMockProducer = new BaseMetricHelper(jobConfig) {
      override val metricsProducer: KafkaMessageProducer = mockMetricsProducer
    }
    baseMetricHelperWithMockProducer.generate(System.currentTimeMillis(), dataset.get.id, edata)
  }

  "KafkaMessageProducer" should "send a message to Kafka" in {
    val defaultTopic = "spark.stats"
    val defaultKey = "test-key"
    val message = """{"eid":"METRIC","ets":1701854838310,"mid":"51d9fecd-6654-4f16-ad1d-dc6204826d79","actor":{"id":"MasterDataProcessorIndexerJob","type":"SYSTEM"},"context":{"env":"local","pdata":{"id":"DataProducts","pid":"MasterDataProcessorIndexerJob","ver":"1.0.0"}},"object":{"id":"md1","type":"Dataset","ver":"1.0.0"},"edata":{"metric":{"key":"value"},"labels":[{"key":"label-key","value":"label-value"}]}}"""
    val mockProducer = mock[org.apache.kafka.clients.producer.Producer[String, String]]
    val kafkaMessageProducer = KafkaMessageProducer(jobConfig)

    kafkaMessageProducer.sendMessage(message = message)
    val record = new ProducerRecord[String, String](defaultTopic, defaultKey, message)
    verify(mockProducer.send(record))
  }

  "KafkaMessageProducer" should "throw error if an exception occurs while sending a message" in {
    val message = """{"eid":"METRIC","ets":1701854838310,"mid":"51d9fecd-6654-4f16-ad1d-dc6204826d79","actor":{"id":"MasterDataProcessorIndexerJob","type":"SYSTEM"},"context":{"env":"local","pdata":{"id":"DataProducts","pid":"MasterDataProcessorIndexerJob","ver":"1.0.0"}},"object":{"id":"md1","type":"Dataset","ver":"1.0.0"},"edata":{"metric":{"key":"value"},"labels":[{"key":"label-key","value":"label-value"}]}}"""
    val mockedProducer = mock[KafkaProducer[String, String]]

    val exception = intercept[Exception] {
      val record = new ProducerRecord[String, String](null, null, message)
      mockedProducer.send(record)
    }

    exception.getMessage should include("Topic cannot be null.")
  }
}

