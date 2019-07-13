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
        val topic = "first_topic"
        val value = "Bringal $i"
        val key = "id_$i"
        logger.info("Key $key")


        val record = ProducerRecord<String, String>(topic,key,value)
        // this is an async call
        producer.send(record) { recordMetadata, exception ->
            if (exception == null) {
                logger.info("Received new metadata: \n" +
                        "Partition: ${recordMetadata.partition()}\n"+
                        "Topic: ${recordMetadata.topic()} \n" +
                        "Offsets: ${recordMetadata.offset()} \n" +
                        "Timestamp: ${recordMetadata.timestamp()}")

            } else {
                logger.error("Error while producing", exception)
            }
        }.get() // block the send to make it synchronous
        //id_1 -> 0 // same key goes to the same partition no matter howmany times the code is run on the same broker
        //id_2 => 2
        //id_3 => 0
        //id_4 => 2
        //id_8 => 1
    }
    producer.flush()
    producer.close()

}