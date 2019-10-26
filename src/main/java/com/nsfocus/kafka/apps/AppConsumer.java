package com.nsfocus.kafka.apps;

import com.nsfocus.kafka.constants.IKafkaConstants;
import com.nsfocus.kafka.consumer.ConsumerCreator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class AppConsumer {
    public static void main(String[] args) {
        AppConsumer appConsumer = new AppConsumer();
        appConsumer.runConsumer();

    }

    public void consumAtbegin() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
        int noMessageToFetch = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(100); //加入组
/*            TopicPartition topicPartition;
            ArrayList<TopicPartition> partitions = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                topicPartition = new TopicPartition(IKafkaConstants.TOPIC_NAME, i);
                partitions.add(topicPartition);
            }
            topicPartition = new TopicPartition(IKafkaConstants.TOPIC_NAME, 0);
            consumer.seek(topicPartition,2);
//            consumer.seekToBeginning(partitions);*/
            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }

            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
    }
    public void runConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

        int noMessageToFetch = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(10);
            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }

            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
    }
}
