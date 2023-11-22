package org.sunbird.obsrv.streaming

import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.streaming._
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.registry.DatasetRegistry

import java.lang
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import scala.collection.mutable

abstract class BaseDatasetProcessFunction(config: BaseJobConfig[mutable.Map[String, AnyRef]]) extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  def getMetrics(): List[String]

  override def getMetricsList(): MetricsList = {
    val metrics = getMetrics() ++ List(config.eventFailedMetricsCount)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }

  private def initMetrics(datasetId: String): Unit = {
    if(!metrics.hasDataset(datasetId)) {
      val metricMap = new ConcurrentHashMap[String, AtomicLong]()
      metricsList.metrics.map(metric => {
        metricMap.put(metric, new AtomicLong(0L))
        getRuntimeContext.getMetricGroup.addGroup(config.jobName).addGroup(datasetId)
          .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(datasetId, metric)))
      })
      metrics.initDataset(datasetId, metricMap)
    }
  }

  def processElement(dataset: Dataset, event: mutable.Map[String, AnyRef],context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context, metrics: Metrics): Unit
  override def processElement(event: mutable.Map[String, AnyRef], context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context, metrics: Metrics): Unit = {

    val datasetIdOpt = event.get(config.CONST_DATASET)
    if (datasetIdOpt.isEmpty) {
      context.output(config.failedEventsOutputTag(), markFailed(event, ErrorConstants.MISSING_DATASET_ID, config.jobName))
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      return
    }
    val datasetId = datasetIdOpt.get.asInstanceOf[String]
    initMetrics(datasetId)
    val datasetOpt = DatasetRegistry.getDataset(datasetId)
    if (datasetOpt.isEmpty) {
      context.output(config.failedEventsOutputTag(), markFailed(event, ErrorConstants.MISSING_DATASET_CONFIGURATION, config.jobName))
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      return
    }
    if (!super.containsEvent(event)) {
      metrics.incCounter(datasetId, config.eventFailedMetricsCount)
      context.output(config.failedEventsOutputTag(), markFailed(event, ErrorConstants.EVENT_MISSING, config.jobName))
      return
    }
    processElement(datasetOpt.get, event, context, metrics)
  }
}

abstract class BaseDatasetWindowProcessFunction(config: BaseJobConfig[mutable.Map[String, AnyRef]]) extends WindowBaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String](config) {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  def getMetrics(): List[String]

  override def getMetricsList(): MetricsList = {
    val metrics = getMetrics() ++ List(config.eventFailedMetricsCount)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }

  private def initMetrics(datasetId: String): Unit = {
    if(!metrics.hasDataset(datasetId)) {
      val metricMap = new ConcurrentHashMap[String, AtomicLong]()
      metricsList.metrics.map(metric => {
        metricMap.put(metric, new AtomicLong(0L))
        getRuntimeContext.getMetricGroup.addGroup(config.jobName).addGroup(datasetId)
          .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(datasetId, metric)))
      })
      metrics.initDataset(datasetId, metricMap)
    }
  }

  def processWindow(dataset: Dataset, context: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context, elements: List[mutable.Map[String, AnyRef]], metrics: Metrics): Unit
  override def process(datasetId: String, context: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context, elements: lang.Iterable[mutable.Map[String, AnyRef]], metrics: Metrics): Unit = {

    initMetrics(datasetId)
    val datasetOpt = DatasetRegistry.getDataset(datasetId)
    val eventsList = elements.asScala.toList
    if (datasetOpt.isEmpty) {
      eventsList.foreach(event => {
        context.output(config.failedEventsOutputTag(), markFailed(event, ErrorConstants.MISSING_DATASET_CONFIGURATION, config.jobName))
        metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      })
      return
    }
    val buffer = mutable.Buffer[mutable.Map[String, AnyRef]]()
    eventsList.foreach(event => {
      if (!super.containsEvent(event)) {
        metrics.incCounter(datasetId, config.eventFailedMetricsCount)
        context.output(config.failedEventsOutputTag(), markFailed(event, ErrorConstants.EVENT_MISSING, config.jobName))
      } else {
        buffer.append(event)
      }
    })

    if(buffer.nonEmpty) {
      processWindow(datasetOpt.get, context, buffer.toList, metrics)
    }
  }
}
