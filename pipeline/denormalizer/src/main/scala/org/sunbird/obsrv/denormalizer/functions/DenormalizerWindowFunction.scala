package org.sunbird.obsrv.denormalizer.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.core.util.Util
import org.sunbird.obsrv.denormalizer.task.DenormalizerConfig
import org.sunbird.obsrv.denormalizer.util._
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.streaming.BaseDatasetWindowProcessFunction

import scala.collection.mutable

class DenormalizerWindowFunction(config: DenormalizerConfig)(implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]])
  extends BaseDatasetWindowProcessFunction(config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DenormalizerWindowFunction])

  private[this] var denormCache: DenormCache = _

  override def getMetrics(): List[String] = {
    List(config.denormSuccess, config.denormTotal, config.denormFailed, config.eventsSkipped, config.denormPartialSuccess)
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

  override def processWindow(dataset: Dataset, context: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context, elements: List[mutable.Map[String, AnyRef]], metrics: Metrics): Unit = {

    metrics.incCounter(dataset.id, config.denormTotal, elements.size.toLong)
    denormCache.open(dataset)
    val denormEvents = elements.map(msg => {
      DenormEvent(msg, None, None)
    })

    if (dataset.denormConfig.isDefined) {
      denormalize(denormEvents, dataset, metrics, context)
    } else {
      metrics.incCounter(dataset.id, config.eventsSkipped, elements.size.toLong)
      elements.foreach(msg => {
        context.output(config.denormEventsTag, markSkipped(msg, config.jobName))
      })
    }
  }

  private def denormalize(events: List[DenormEvent], dataset: Dataset, metrics: Metrics,
                          context: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context): Unit = {

    val datasetId = dataset.id
    val denormEvents = denormCache.denormMultipleEvents(datasetId, events, dataset.denormConfig.get.denormFields)
    denormEvents.foreach(denormEvent => {
      val status = getDenormStatus(denormEvent)
      context.output(config.denormEventsTag, markStatus(denormEvent.msg, config.jobName, status))
      status match {
        case "success" => metrics.incCounter(datasetId, config.denormSuccess)
        case "failed" => metrics.incCounter(datasetId, config.denormFailed)
        case "partial-success" => metrics.incCounter(datasetId, config.denormPartialSuccess)
        case "skipped" => metrics.incCounter(datasetId, config.eventsSkipped)
      }
    })
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