package org.sunbird.obsrv.preprocessor.functions

import com.github.fge.jsonschema.core.report.ProcessingReport
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.Models.{PData, SystemEvent}
import org.sunbird.obsrv.core.model.{Constants, ErrorConstants}
import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.model.DatasetStatus
import org.sunbird.obsrv.preprocessor.task.PipelinePreprocessorConfig
import org.sunbird.obsrv.preprocessor.util.SchemaValidator
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.streaming.BaseDatasetProcessFunction

import scala.collection.mutable

class EventValidationFunction(config: PipelinePreprocessorConfig, @transient var schemaValidator: SchemaValidator = null)
                             (implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]])
  extends BaseDatasetProcessFunction(config) {
  private[this] val logger = LoggerFactory.getLogger(classOf[EventValidationFunction])

  override def getMetrics(): List[String] = {
    List(config.validationTotalMetricsCount, config.validationFailureMetricsCount, config.validationSuccessMetricsCount,
      config.validationSkipMetricsCount, config.eventIgnoredMetricsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (schemaValidator == null) {
      schemaValidator = new SchemaValidator(config)
      schemaValidator.loadDataSchemas(DatasetRegistry.getAllDatasets(config.datasetType()))
    }
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(dataset: Dataset, msg: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    metrics.incCounter(config.defaultDatasetID, config.validationTotalMetricsCount)
    if (dataset.status != DatasetStatus.Live) {
      metrics.incCounter(dataset.id, config.eventIgnoredMetricsCount)
      return
    }
    val validationConfig = dataset.validationConfig
    if (validationConfig.isDefined && validationConfig.get.validate.get) {
      schemaValidator.loadDataSchema(dataset)
      validateEvent(dataset, msg, context, metrics)
    } else {
      metrics.incCounter(dataset.id, config.validationSkipMetricsCount)
      context.output(config.validEventsOutputTag, markSkipped(msg, "EventValidation"))
    }
  }

  private def validateEvent(dataset: Dataset, msg: mutable.Map[String, AnyRef],
                            context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                            metrics: Metrics): Unit = {

    val event = msg(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]]
    try {
      if (schemaValidator.schemaFileExists(dataset)) {
        val validationReport = schemaValidator.validate(dataset.id, event)
        if (validationReport.isSuccess) {
          onValidationSuccess(dataset, msg, metrics, context)
        } else {
          onValidationFailure(dataset, msg, metrics, context, validationReport)
        }
      }
    } catch {
      case ex: ObsrvException =>
        logger.error("EventValidationFunction:validateEvent()-Exception: ", ex.getMessage)
        ex.printStackTrace()
        metrics.incCounter(dataset.id, config.validationFailureMetricsCount)
        context.output(config.failedEventsOutputTag(), markFailed(msg, ex.error, "EventValidation"))
    }
  }

  private def onValidationSuccess(dataset: Dataset, event: mutable.Map[String, AnyRef], metrics: Metrics,
                                  context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context): Unit = {
    metrics.incCounter(dataset.id, config.validationSuccessMetricsCount)
    context.output(config.validEventsOutputTag, markSuccess(event, "EventValidation"))
  }

  private def onValidationFailure(dataset: Dataset, event: mutable.Map[String, AnyRef], metrics: Metrics,
                                  context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                                  validationReport: ProcessingReport): Unit = {
    var errorReasons: Array[Any] = Array()

    validationReport.forEach(failure => {
      val failureMessage = failure.getMessage
      val failureJSON = failure.asJson()
      val node = failureJSON.get(Constants.LEVEL).asText()
      if (node == Constants.ERROR) {
        val errorType = failureJSON.get("keyword").asText()
        val field = failureJSON.get("schema").get("pointer").asText()
        errorType match {
          case "type" =>
            val expectedType = failureJSON.get("expected").toString
            val actualType = failureJSON.get("found").asText()
            val finalErrorMessage = s"Expected data type for $field is $expectedType but found $actualType instead"
            errorReasons = errorReasons :+ finalErrorMessage

          case "required" =>
            val missingFields = failureJSON.get("missing").toString
            val finalErrorMessage = s"Missing required field(s) $missingFields"
            errorReasons = errorReasons :+ finalErrorMessage

          case _ =>
            val finalErrorMessage = s"$failureMessage for field $field"
            errorReasons = errorReasons :+ finalErrorMessage
        }
      }
    })

    metrics.incCounter(dataset.id, config.validationFailureMetricsCount)
    context.output(config.invalidEventsOutputTag, markFailed(event, ErrorConstants.SCHEMA_VALIDATION_FAILED.copy(errorReason = Some(JSONUtil.serialize(errorReasons))), "EventValidation"))
    val systemEvent = SystemEvent(PData(config.jobName, "flink", "validation"), Map(Constants.ERROR_CODE -> ErrorConstants.SCHEMA_VALIDATION_FAILED.errorCode, Constants.ERROR_MSG -> "Validation Failure", Constants.ERROR_REASON -> JSONUtil.serialize(errorReasons)))
    context.output(config.systemEventsOutputTag, JSONUtil.serialize(systemEvent))
  }

}