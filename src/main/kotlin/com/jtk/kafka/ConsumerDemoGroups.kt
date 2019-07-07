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
    val groupId = "my-fourth-application"
    val bootstrapServer = "127.0.0.1:9092"
    val topic = "first_topic"

    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer::class.java.canonicalName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer::class.java.canonicalName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")

    val consumer = KafkaConsumer<String,String>(properties)
    consumer.subscribe(listOf(topic))

    while (true){
        val consumerRecords = consumer.poll(Duration.ofMillis(100))
         for (consumerRecord in consumerRecords){
             logger.info("Key: ${consumerRecord.key()}, Value: ${consumerRecord.value()}")
             logger.info("Partition: ${consumerRecord.partition()}, Offset: ${consumerRecord.offset()}")
         }
    }

}