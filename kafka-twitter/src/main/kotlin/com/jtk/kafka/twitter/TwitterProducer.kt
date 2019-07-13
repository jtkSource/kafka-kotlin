package com.jtk.kafka.twitter

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.event.Event
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

val logger = LoggerFactory.getLogger("TwitterProducer-main")!!

class TwitterProducer {

    private var terms: ArrayList<String> = arrayListOf("kafka", "java","bitcoin","politics","sports")

    fun run() {

        val msgQueue = LinkedBlockingQueue<String>(100000)

        createTwitterClient(msgQueue)?.let {
            it.connect()
            val producer = createKafkaProducer()
            Runtime.getRuntime().addShutdownHook(Thread {
                logger.info("Shutting down application...")
                it.stop()
                producer.close()
                logger.info("closed connections...")

            })

            publishTweet(it, msgQueue, producer)
        }



        logger.info("End Of Application")
    }

    private fun createKafkaProducer(): KafkaProducer<String, String> {

        val props = Properties()
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)

        //create safe producers
        props[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = "true"
        props[ProducerConfig.ACKS_CONFIG] = "all"
        props[ProducerConfig.RETRIES_CONFIG] = Int.MAX_VALUE.toString()
        props[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = "5"

        // high throughput
        props[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "snappy"
        props[ProducerConfig.LINGER_MS_CONFIG] = "20"
        props[ProducerConfig.BATCH_SIZE_CONFIG] = (32 * 1024).toString()

        return KafkaProducer(props)
    }

    private fun publishTweet(client: BasicClient, msgQueue: LinkedBlockingQueue<String>, producer: KafkaProducer<String, String>) {
        while (!client.isDone) {
            try {
                var msg = msgQueue.poll(5, TimeUnit.SECONDS)
                msg?.let {
                    producer.send(ProducerRecord("twitter_tweets", null, it), Callback { recordMetadata, exception ->
                        if (exception != null) {
                            logger.error("Unexpected Exception", exception)
                        } else {
                            logger.info(recordMetadata.topic())
                        }
                    })
                }
            } catch (e: Exception) {
                e.printStackTrace()
                client.stop()
            }
        }
    }


    private fun createTwitterClient(msgQueue: LinkedBlockingQueue<String>): BasicClient? {
        val eventQueue = LinkedBlockingQueue<Event>(1000)

        val hosebirdHosts = HttpHosts(Constants.STREAM_HOST)
        val hosebirdEndpoint = StatusesFilterEndpoint()
        hosebirdEndpoint.trackTerms(terms)
        val hoseBirdoseAuth = OAuth1("AYBC3Cas5VdliONEeC6LDV0Ch", "qd3oOqPC8XkEZhnlRNgOHujKI92vzOd311RsPND9o6ht5k1l1F",
                "1740067440-RxApJHn31grITSXXE1dGgkTjDfRAQdvUpSqPcdZ", "arBKTU8o39pw4g6hrhBZAP4AClOnIkPZ5fb6Bdh9KPgGS")

        val builder = ClientBuilder().name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hoseBirdoseAuth)
                .endpoint(hosebirdEndpoint)
                .processor(StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue)
        return builder.build()

    }
}


fun main() {
    TwitterProducer().let { it.run() }
}