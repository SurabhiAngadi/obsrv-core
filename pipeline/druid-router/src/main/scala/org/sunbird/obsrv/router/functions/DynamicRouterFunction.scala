package org.sunbird.obsrv.router.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.model.Constants
import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.core.util.Util
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.router.task.DruidRouterConfig
import org.sunbird.obsrv.streaming.BaseDatasetProcessFunction

import scala.collection.mutable

class DynamicRouterFunction(config: DruidRouterConfig) extends BaseDatasetProcessFunction(config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DynamicRouterFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def getMetrics(): List[String] = {
    List(config.routerTotalCount, config.routerSuccessCount)
  }

  override def processElement(dataset: Dataset, msg: mutable.Map[String, AnyRef],
                              ctx: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    metrics.incCounter(dataset.id, config.routerTotalCount)
    val event = Util.getMutableMap(msg(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]])
    event.put(config.CONST_OBSRV_META, msg(config.CONST_OBSRV_META))
    val routerConfig = dataset.routerConfig
    val topicEventMap = mutable.Map(Constants.TOPIC -> routerConfig.topic, Constants.MESSAGE -> event)
    ctx.output(config.routerOutputTag, topicEventMap)
    metrics.incCounter(dataset.id, config.routerSuccessCount)

    msg.remove(config.CONST_EVENT)
    ctx.output(config.statsOutputTag, markComplete(msg, dataset.dataVersion))
  }
}
