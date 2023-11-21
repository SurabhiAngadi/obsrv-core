package org.sunbird.obsrv.denormalizer.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.core.util.Util
import org.sunbird.obsrv.denormalizer.task.DenormalizerConfig
import org.sunbird.obsrv.denormalizer.util.{DenormCache, DenormEvent}
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable

class DenormalizerFunction(config: DenormalizerConfig)
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DenormalizerFunction])

  private[this] var denormCache: DenormCache = _

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.denormSuccess, config.denormTotal, config.denormFailed, config.eventsSkipped)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    denormCache = new DenormCache(config)
    denormCache.open(DatasetRegistry.getAllDatasets(config.datasetType()))
  }

  override def close(): Unit = {
    super.close()
    denormCache.close()
  }

  override def processElement(msg: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    val datasetId = msg(config.CONST_DATASET).asInstanceOf[String] // DatasetId cannot be empty at this stage
    metrics.incCounter(datasetId, config.denormTotal)
    val dataset = DatasetRegistry.getDataset(datasetId).get
    denormCache.open(dataset)
    if (dataset.denormConfig.isDefined) {
      val event = DenormEvent(msg, None, None)
      val denormEvent = denormCache.denormEvent(datasetId, event, dataset.denormConfig.get.denormFields)
      val status = getDenormStatus(denormEvent)
      context.output(config.denormEventsTag, markStatus(denormEvent.msg, config.jobName, status))
      status match {
        case "success" => metrics.incCounter(datasetId, config.denormSuccess)
        case "failed" => metrics.incCounter(datasetId, config.denormFailed)
        case "partial-success" => metrics.incCounter(datasetId, config.denormPartialSuccess)
        case "skipped" => metrics.incCounter(datasetId, config.eventsSkipped)
      }
    } else {
      metrics.incCounter(datasetId, config.eventsSkipped)
      context.output(config.denormEventsTag, markSkipped(msg, config.jobName))
    }
  }

  private def getDenormStatus(denormEvent: DenormEvent): String = {
    if (denormEvent.fieldStatus.isDefined) {
      val totalFieldsCount = denormEvent.fieldStatus.get.size
      val successCount = denormEvent.fieldStatus.get.values.count(f => f.success)
      if (totalFieldsCount == successCount) "success" else if (successCount > 0) "partial-success" else "failed"
    } else {
      "skipped"
    }
  }

  private def markStatus(event: mutable.Map[String, AnyRef], jobName: String, status: String): mutable.Map[String, AnyRef] = {
    val obsrvMeta = Util.getMutableMap(event("obsrv_meta").asInstanceOf[Map[String, AnyRef]])
    addFlags(obsrvMeta, Map(jobName -> status))
    addTimespan(obsrvMeta, jobName)
    event.put("obsrv_meta", obsrvMeta.toMap)
    event
  }

}
