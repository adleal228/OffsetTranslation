package io.confluent.translation;

import io.confluent.translation.model.RecordMetaData;
import io.confluent.translation.utils.KafkaUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

//import kafka.coordinator.group.OffsetKey;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.Producer;
//
//
//import org.apache.kafka.common.*;
public class OskToCCMigration {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println(" Hello World !! ");

        String bootstrapServers = "localhost:9092";
        String topicName = "sourceTopic";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.put(ConsumerConfig.GROUP_ID_CONFIG,"CG-Test-with-poll-api");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Migration-Tool-CG5");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "jaggib-laptop");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer consumerConsumerOffsets = new KafkaConsumer(properties);
        KafkaConsumer consumer = new KafkaConsumer(properties);

        consumerConsumerOffsets.subscribe(Arrays.asList("__consumer_offsets"));
        ConsumerRecords<String, String> consumerOffsetRecords = consumerConsumerOffsets.poll(Duration.ofSeconds(100));
        System.out.println(" Current offset status from __consumer_offsets topic");

        AdminClient adminClient = AdminClient. create(properties);
        Map<Integer,RecordMetaData>   recordMetaDataMap = new HashMap<Integer, RecordMetaData>();

        System.out.println(" All COnsumer Groups ");
        List<String> groupIds = adminClient.listConsumerGroups().all().get().
                stream().map(s -> s.groupId()).collect(Collectors.toList());

        groupIds.forEach(groupId -> {
            System.out.println("Summarizing Consumer Group : " + groupId);

            try {
                Map<TopicPartition, OffsetAndMetadata> list = adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();


                list.forEach((topicPartition, offsetAndMetadata) -> {
                    consumer.assign(Arrays.asList(topicPartition));
                    consumer.seek(topicPartition, offsetAndMetadata.offset());
                    consumer.position(topicPartition);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord record : records) {
                        RecordMetaData recordMetaData = new RecordMetaData();
                        recordMetaData.setOffset(offsetAndMetadata.offset());
                        recordMetaData.setPartition(topicPartition);
                        recordMetaData.setTimestamp(record.timestamp());

                        recordMetaDataMap.put(record.partition(),recordMetaData );

                        System.out.println( " Partition : "+topicPartition + " committed offset "+offsetAndMetadata.offset() + ", timestamp :"+recordMetaData.getTimestamp());

                    }

                });
                System.out.println();
                System.out.println(" Printing contents of the Map ");
                for (Map.Entry<Integer, RecordMetaData> entry : recordMetaDataMap.entrySet()) {
                    System.out.println(entry.getKey() + ":" + entry.getValue().toString());
                }

                System.out.println();
                System.out.println();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            //System.out.println(list);
        });

        //adminClient.alterConsumerGroupOffsets();
        // Alter offsets

        Properties confluentCloudProps = KafkaUtils.getPrimarySiteConfigs();
        confluentCloudProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        confluentCloudProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        System.out.println(" Resetting CG on Confluent Cloud ");
        KafkaConsumer cloudConsumer = new KafkaConsumer(confluentCloudProps);
        final Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
        for (Map.Entry<Integer, RecordMetaData> entry : recordMetaDataMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue().toString());
            timestampsToSearch.put(entry.getValue().getPartition(), entry.getValue().getTimestamp());
        }

        result = cloudConsumer.offsetsForTimes(timestampsToSearch);
        // Seek to the specified offset for each partition
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : result.entrySet())
        {
            if(entry.getValue() != null && entry.getValue().offset()!= -1) {
                System.out.println(" Resetting CG on Confluent Cloud, partition " + entry.getKey() + " , offset " + entry.getValue().offset());
                cloudConsumer.seek(entry.getKey(), entry.getValue().offset());
            } else {
                System.out.println(" Did nothing to Confluent Cloud, partition " + entry.getKey() );
            }

        }


        //System.out.println(adminClient.listConsumerGroupOffsets("restTool2").partitionsToOffsetAndMetadata().get());

//            consumer.commitSync();
//
//                    while (true) {
//                        ConsumerRecords<String, String> recordsFinal = consumer.poll(Duration.ofSeconds(10));
//                        recordsFinal.forEach(lastRecord -> {
//                            System.out.println( "Partition : " + lastRecord.partition() + " Offset : "+ lastRecord.offset() + ", TimeStamp " +  lastRecord.timestamp());
//                        });
//                    }
//                }



    }
}