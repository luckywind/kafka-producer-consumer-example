package com.nsfocus.kafka.apps;

import com.nsfocus.kafka.App;
import com.nsfocus.kafka.constants.IKafkaConstants;
import com.nsfocus.kafka.producer.ProducerCreator;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

public class AppProducer {
    public static void main(String[] args) {
        AppProducer appProducer = new AppProducer();
        appProducer.runProducer();
    }
    public  void runProducer() {
        Producer<Long, String> producer = ProducerCreator.createProducer();

        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,
                    "This is record " + index);
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (ExecutionException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }
}
