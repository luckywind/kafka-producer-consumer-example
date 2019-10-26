package com.nsfocus.kafka.offset;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import java.util.*;

public class Consumer {
    private static Scanner in;

    public static void main(String[] argv)throws Exception{
        if (argv.length != 3) {
            System.err.printf("Usage: %s <topicName> <groupId> <startingOffset>\n",
                    Consumer.class.getSimpleName());
            System.exit(-1);
        }
        in = new Scanner(System.in);

        String topicName = argv[0];
        String groupId = argv[1];
        final long startingOffset = Long.parseLong(argv[2]);

        ConsumerThread consumerThread = new ConsumerThread(topicName,groupId,startingOffset);
        consumerThread.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        /**
         * poll(int >0)时，consumer会阻塞直到拉取到一条消息，如果想让consumer停下来，
         * 需要从另一个线程调用consumer的 wakeup方法(这会抛出异常)
         */
        consumerThread.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerThread.join();

    }

    private static class ConsumerThread extends Thread{
        private String topicName;
        private String groupId;
        private long startingOffset;
        private KafkaConsumer<String,String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId, long startingOffset){
            this.topicName = topicName;
            this.groupId = groupId;
            this.startingOffset=startingOffset;
        }
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.67.1.222:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "offset123");
            configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            /**
             * What to do when there is no initial offset in Kafka or if the current
             * offset does not exist any more on the server (e.g. because that data has been deleted):
             *     earliest: automatically reset the offset to the earliest offset<li>
             *         latest: automatically reset the offset to the latest offset
             *    none: throw exception to the consumer
             * if no previous offset is found for the consumer's group</li>
             * <li>anything else: throw exception to the consumer
             */
            configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            /**
             * consumer会跟踪属于同一个group的consumer列表，以下事件会触发rebalance:
             * Number of partitions change for any of the subscribed topics
             * A subscribed topic is created or deleted
             * An existing member of the consumer group is shutdown or fails
             * A new member is added to the consumer group
             *
             * rebalance会触发listener
             *
             */
            kafkaConsumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
                //rebalance之前调用，consumer停止拉取数据时调用，建议在这里处理offset提交
                //参数是rebalance之前该consumer分配的partitions
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.printf("%s topic-partitions 从这个 consumer 撤销\n", Arrays.toString(partitions.toArray()));
                }

                //partition重分配完成时、开始拉取数据时会调用，可以在这里处理offset。
                //参数是rebanlance后分配到该consumer的partitions
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.printf("%s topic-partitions 分配给 这个consumer\n", Arrays.toString(partitions.toArray()));
                    Iterator<TopicPartition> topicPartitionIterator = partitions.iterator();
                    while (topicPartitionIterator.hasNext()) {
                        TopicPartition topicPartition = topicPartitionIterator.next();
                        System.out.println("Current offset is " + kafkaConsumer.position(topicPartition) + " committed offset is ->" + kafkaConsumer.committed(topicPartition));
                        if (startingOffset == -2) {
                            System.out.println("Leaving it alone");
                        } else if (startingOffset == 0) {
                            System.out.println("Setting offset to begining");

                            kafkaConsumer.seekToBeginning(Collections.singleton(topicPartition));
                        } else if (startingOffset == -1) {
                            System.out.println("Setting it to the end ");

                            kafkaConsumer.seekToEnd(Collections.singleton(topicPartition));
                        } else {
                            System.out.println("Resetting offset to " + startingOffset);
                            kafkaConsumer.seek(topicPartition, startingOffset);
                        }
                    }
                }
            });
            //Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(record.value());
                    }
                    if(startingOffset == -2)
                        kafkaConsumer.commitSync();
                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            }finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }
        public KafkaConsumer<String,String> getKafkaConsumer(){
            return this.kafkaConsumer;
        }
    }
}