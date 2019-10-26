package com.nsfocus.kafka.offset;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.ConsumerMetadataResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.network.BlockingChannel;

import java.io.IOException;
import java.util.*;
public class OffsetManager {
    //https://cwiki.apache.org/confluence/display/KAFKA/Committing+and+fetching+consumer+offsets+in+Kafka
    public static void main(String[] args) {

    }

    public static void fetchConnectManager() {
        BlockingChannel channel = new BlockingChannel("localhost", 9092,
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                5000 /* read timeout in millis */);
        channel.connect();
        final String MY_GROUP = "demoGroup";
        final String MY_CLIENTID = "demoClientId";
        int correlationId = 0;
        final TopicAndPartition testPartition0 = new TopicAndPartition("demoTopic", 0);
        final TopicAndPartition testPartition1 = new TopicAndPartition("demoTopic", 1);
        channel.send(new ConsumerMetadataRequest(MY_GROUP, ConsumerMetadataRequest.CurrentVersion(), correlationId++, MY_CLIENTID));
        ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

        if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
            Broker offsetManager = metadataResponse.coordinator();
            // if the coordinator is different, from the above channel's host then reconnect
            channel.disconnect();
            channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                    BlockingChannel.UseDefaultBufferSize(),
                    BlockingChannel.UseDefaultBufferSize(),
                    5000 /* read timeout in millis */);
            channel.connect();
        } else {
            // retry (after backoff)
        }
    }

    public static void fetchOffset() {

    }





}
