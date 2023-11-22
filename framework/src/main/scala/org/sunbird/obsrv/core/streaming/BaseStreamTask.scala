package org.sunbird.obsrv.core.streaming


import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.util.Properties
import scala.collection.mutable

abstract class BaseStreamTask[T] {

  def process()

  def processStream(dataStream: DataStream[T]): DataStream[T]

  def addDefaultSinks(dataStream: SingleOutputStreamOperator[T], config: BaseJobConfig[T], kafkaConnector: FlinkKafkaConnector): DataStreamSink[T] = {
    dataStream.getSideOutput(config.failedEventsOutputTag()).sinkTo(kafkaConnector.kafkaSink[T](config.kafkaFailedTopic))
      .name(config.failedEventProducer).uid(config.failedEventProducer).setParallelism(config.downstreamOperatorsParallelism)
  }

  def getMapDataStream(env: StreamExecutionEnvironment, config: BaseJobConfig[T], kafkaConnector: FlinkKafkaConnector): DataStream[mutable.Map[String, AnyRef]] = {
    env.fromSource(kafkaConnector.kafkaMapSource(config.inputTopic()), WatermarkStrategy.noWatermarks[mutable.Map[String, AnyRef]](), config.inputConsumer())
      .uid(config.inputConsumer()).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
  }

  def getMapDataStream(env: StreamExecutionEnvironment, config: BaseJobConfig[T], kafkaTopics: List[String],
                       kafkaConsumerProperties: Properties, consumerSourceName: String, kafkaConnector: FlinkKafkaConnector): DataStream[mutable.Map[String, AnyRef]] = {
    env.fromSource(kafkaConnector.kafkaMapSource(kafkaTopics, kafkaConsumerProperties), WatermarkStrategy.noWatermarks[mutable.Map[String, AnyRef]](), consumerSourceName)
      .uid(consumerSourceName).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
  }

  def getStringDataStream(env: StreamExecutionEnvironment, config: BaseJobConfig[T], kafkaConnector: FlinkKafkaConnector): DataStream[String] = {
    env.fromSource(kafkaConnector.kafkaStringSource(config.inputTopic()), WatermarkStrategy.noWatermarks[String](), config.inputConsumer())
      .uid(config.inputConsumer()).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
  }

  def getStringDataStream(env: StreamExecutionEnvironment, config: BaseJobConfig[T], kafkaTopics: List[String],
                          kafkaConsumerProperties: Properties, consumerSourceName: String, kafkaConnector: FlinkKafkaConnector): DataStream[String] = {
    env.fromSource(kafkaConnector.kafkaStringSource(kafkaTopics, kafkaConsumerProperties), WatermarkStrategy.noWatermarks[String](), consumerSourceName)
      .uid(consumerSourceName).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
  }

}
