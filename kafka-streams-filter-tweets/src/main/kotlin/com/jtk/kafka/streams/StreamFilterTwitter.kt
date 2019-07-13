package com.jtk.kafka.streams

import com.google.gson.JsonParser
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Predicate
import java.util.*


val jsonParser = JsonParser()

fun extractUserFollowersInTweet(jsonTweet: String): Int {
    val followers = jsonParser.parse(jsonTweet).asJsonObject.get("user").asJsonObject?.get("followers_count")
    return if (followers == null) 0 else followers.asInt
}

fun main() {
    val props = Properties()
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "demo-kafka-streams" // similar to consumer group
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.StringSerde::class.java.name
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.StringSerde::class.java.name

    //create topology
    val streamsBuilder = StreamsBuilder()
    // input topic
    val inputTopic = streamsBuilder.stream<String, String>("twitter_tweets")
    val filteredStream = inputTopic.filter { k, jsonTweet ->
        extractUserFollowersInTweet(jsonTweet) > 10000
    }
    filteredStream.to("important_tweets")

    //build the topology
    val kafkaStreams = KafkaStreams(streamsBuilder.build(),props)

    //start
    kafkaStreams.start()

}