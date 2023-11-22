package org.sunbird.obsrv.denormalizer.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.core.util.Util
import org.sunbird.obsrv.denormalizer.task.DenormalizerConfig
import org.sunbird.obsrv.denormalizer.util.{DenormCache, DenormEvent}
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.streaming.BaseDatasetProcessFunction

import scala.collection.mutable

class DenormalizerFunction(config: DenormalizerConfig) extends BaseDatasetProcessFunction(config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DenormalizerFunction])

  private[this] var denormCache: DenormCache = _

  override def getMetrics(): List[String] = {
    List(config.denormSuccess, config.denormTotal, config.denormFailed, config.eventsSkipped)
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

  override def processElement(dataset: Dataset, msg: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    metrics.incCounter(dataset.id, config.denormTotal)
    denormCache.open(dataset)
    if (dataset.denormConfig.isDefined) {
      val event = DenormEvent(msg, None, None)
      val denormEvent = denormCache.denormEvent(dataset.id, event, dataset.denormConfig.get.denormFields)
      val status = getDenormStatus(denormEvent)
      context.output(config.denormEventsTag, markStatus(denormEvent.msg, config.jobName, status))
      status match {
        case "success" => metrics.incCounter(dataset.id, config.denormSuccess)
        case "failed" => metrics.incCounter(dataset.id, config.denormFailed)
        case "partial-success" => metrics.incCounter(dataset.id, config.denormPartialSuccess)
        case "skipped" => metrics.incCounter(dataset.id, config.eventsSkipped)
      }
    } else {
      metrics.incCounter(dataset.id, config.eventsSkipped)
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
