package com.jtk.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread

fun main() {

    val logger = LoggerFactory.getLogger("ConsumerThreadDemo")!!
    val countDownLatch = CountDownLatch(1)
    val consumerThread = ConsumerThreads(countDownLatch, "first_topic")
    val thread = Thread(consumerThread)
    thread.start()
    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Calling shutdown hook")
        consumerThread.shutdown()
        countDownLatch.await()
    })
    countDownLatch.await()

    logger.info("Application has exited")
}

class ConsumerThreads(private val latch: CountDownLatch, topic: String) : Runnable {

    companion object {
        val logger = LoggerFactory.getLogger(this::class.java.simpleName)!!
    }

    private val consumer: KafkaConsumer<String, String>

    init {
        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.canonicalName)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.canonicalName)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-sixth-application")
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        consumer = KafkaConsumer(properties)
        consumer.subscribe(listOf(topic))
    }

    override fun run() {
        try {
            while (true) {
                val consumerRecords = consumer.poll(Duration.ofMillis(100))
                for (consumerRecord in consumerRecords) {
                    logger.info("Key: ${consumerRecord.key()}, Value: ${consumerRecord.value()}")
                    logger.info("Partition: ${consumerRecord.partition()}, Offset: ${consumerRecord.offset()}")
                }
            }
        } catch (e: WakeupException) {
            logger.info("Received Shutdown signal")
        } finally {
            consumer.close()
            latch.countDown()
        }

    }

    fun shutdown() {
        //interrupts the consumer.poll method
        // it will throw WakeupException
        consumer.wakeup()
    }


}