package org.sunbird.obsrv.denormalizer.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.core.util.Util
import org.sunbird.obsrv.denormalizer.task.DenormalizerConfig
import org.sunbird.obsrv.denormalizer.util.DenormCache
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
    val event = Util.getMutableMap(msg(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]])

    if (dataset.denormConfig.isDefined) {
      try {
        msg.put(config.CONST_EVENT, denormCache.denormEvent(datasetId, event, dataset.denormConfig.get.denormFields).toMap)
        metrics.incCounter(datasetId, config.denormSuccess)
        context.output(config.denormEventsTag, markSuccess(msg, config.jobName))
      } catch {
        case ex: ObsrvException =>
          metrics.incCounter(datasetId, config.denormFailed)
          context.output(config.denormFailedTag, markFailed(msg, ex.error, config.jobName))
      }
    } else {
      metrics.incCounter(datasetId, config.eventsSkipped)
      context.output(config.denormEventsTag, markSkipped(msg, config.jobName))
    }
  }

}
