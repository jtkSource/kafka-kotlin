package com.jtk.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

fun main() {

    val logger = LoggerFactory.getLogger("ProducerDemo")

    val props = Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)


    val producer = KafkaProducer<String, String>(props)

    for (i in 1..10) {
        val record = ProducerRecord<String, String>("first_topic", "Hello world $i")
        // this is an async call
        producer.send(record) { recordMetadata, exception ->
            if (exception == null) {
                logger.info("Received new metadata: \n" +
                        "Topic: ${recordMetadata.topic()} \n" +
                        "Offsets: ${recordMetadata.offset()} \n" +
                        "Timestamp: ${recordMetadata.timestamp()}")

            } else {
                logger.error("Error while producing", exception)
            }
        }
    }
    producer.flush()
    producer.close()


}