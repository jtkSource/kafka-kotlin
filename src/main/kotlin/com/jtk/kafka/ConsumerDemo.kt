package com.jtk.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

fun main() {
    val logger = LoggerFactory.getLogger("ConsumerDemo")
    val properties = Properties()

    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer::class.java.canonicalName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer::class.java.canonicalName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-fourth-application")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")

    val consumer = KafkaConsumer<String,String>(properties)
    consumer.subscribe(listOf("first_topic"))

    while (true){
        val consumerRecords = consumer.poll(Duration.ofMillis(100))
         for (consumerRecord in consumerRecords){
             logger.info("Key: ${consumerRecord.key()}, Value: ${consumerRecord.value()}")
             logger.info("Partition: ${consumerRecord.partition()}, Offset: ${consumerRecord.offset()}")
         }
    }

}