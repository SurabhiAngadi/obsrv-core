package org.sunbird.obsrv.dataproducts.util

import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.{DataSource, Dataset}
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable

object CommonUtils {

  def getExecutionTime[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

  def processEvent(value: String, ts: Long) = {
    val json = JSONUtil.deserialize[mutable.Map[String, AnyRef]](value)
    json("obsrv_meta") = mutable.Map[String, AnyRef]("syncts" -> ts.asInstanceOf[AnyRef]).asInstanceOf[AnyRef]
    JSONUtil.serialize(json)
  }

  def fetchDatasource(dataset: Dataset): DataSource = {
    val datasources = DatasetRegistry.getDatasources(dataset.id)
    val filteredDatasource = datasources.get.filter(datasource =>
      !datasource.metadata.isEmpty && !datasource.metadata.get.aggregated
    )
    if (filteredDatasource.isEmpty || filteredDatasource.size > 1) {
      return null
    }
    filteredDatasource.head
  }


}