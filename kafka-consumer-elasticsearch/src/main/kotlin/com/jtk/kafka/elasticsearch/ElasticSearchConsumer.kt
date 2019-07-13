package com.jtk.kafka.elasticsearch

import com.google.gson.JsonParser
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*


var logger = LoggerFactory.getLogger("ElasticSearchConsumer-main")

class ElasticSearchConsumer {

    fun createClient(): RestHighLevelClient {
        var hostname = "jtk-kafka-1064125637.ap-southeast-2.bonsaisearch.net"
        var username = "nt7yc4op8h"
        var password = "25qmm85ck2"

        val credentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(AuthScope.ANY, UsernamePasswordCredentials(username, password))
        val builder = RestClient
                .builder(HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback {
                    it.setDefaultCredentialsProvider(credentialsProvider)
                }
        return RestHighLevelClient(builder)
    }
}


fun createKafkaConsumer(topic: String): KafkaConsumer<String, String> {
    val properties = Properties()
    val groupId = "kafka-demo-elasticsearch"
    val bootstrapServer = "127.0.0.1:9092"

    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.canonicalName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.canonicalName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false" //disable auto-commit
    properties[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = "100"

    val consumer = KafkaConsumer<String, String>(properties)
    consumer.subscribe(arrayListOf(topic))
    return consumer
}

val jsonParser = JsonParser()

fun extractIdFromTweet(record: ConsumerRecord<String, String>): String {
    var value = jsonParser.parse(record.value()).asJsonObject.get("id_str")
    if (value != null) {
        return value.asString
    } else {
        // messages from kafka can be identified by the topic+partition+offset to know if they are unique
        return "${record.topic()}_${record.partition()}_${record.offset()}"
    }
}


fun main() {
    var jsonString = """
        {
            "foo" : "bar"
        }
    """.trimIndent()

    val es = ElasticSearchConsumer()
    val client = es.createClient()


    val consumer = createKafkaConsumer("twitter_tweets")

    while (true) {

        val consumerRecords = consumer.poll(Duration.ofMillis(100))

        val recordCount = consumerRecords.count()

        logger.info("Received $recordCount records")

        val bulkRequest = BulkRequest()


        for (consumerRecord in consumerRecords) {

            jsonString = consumerRecord.value()

            val id: String = extractIdFromTweet(consumerRecord)

            val indexRequest = IndexRequest(
                    "twitter",
                    "tweets",
                    id // this is to make the consumer idemopotent
            ).source(jsonString, XContentType.JSON)

            bulkRequest.add(indexRequest)

        }

        if (recordCount > 0) {
            val bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT)
            logger.info("Committing offset...")
            consumer.commitSync()
            logger.info("Committed offset")
            Thread.sleep(1000)
        }
    }
    client.close()
}


