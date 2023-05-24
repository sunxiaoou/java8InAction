package lambdasinaction.tmp;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Kafka {
    static class KfkAdmin {
        AdminClient admin;

        KfkAdmin(String host, int port) {
            Properties props = new Properties();
            props.put("bootstrap.servers", String.format("%s:%d", host, port));
            admin = AdminClient.create(props);
        }

       List<String> topics() {
            ListTopicsOptions options = new ListTopicsOptions().listInternal(true); // Include internal topics
            ListTopicsResult topics = admin.listTopics(options);
            Set<String> topicNames;
            try {
                topicNames = topics.names().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return null;
            }
            return new ArrayList<>(topicNames);
        }

        String topicDescription(String name) {
            DescribeTopicsResult result = admin.describeTopics(Collections.singleton(name));
            TopicDescription desc;
            try {
                desc = result.topicNameValues().get(name).get();
            } catch (UnknownTopicOrPartitionException | ExecutionException e) {
                return null;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
            return desc.toString();
        }

        void createTopic(String name, int numPartitions, int replicationFactor) {
            if (topicDescription(name) == null) {
                NewTopic newTopic = new NewTopic(name, numPartitions, (short) replicationFactor);
                KafkaFuture<Void> result = admin.createTopics(Collections.singletonList(newTopic)).all();
                try {
                    result.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                System.out.println("Topic created: " + name);
            }
        }

        void deleteTopic(String name) {
            if (topicDescription(name) != null) {
                DeleteTopicsResult deleteResult = admin.deleteTopics(Collections.singleton(name));
                try {
                    deleteResult.all().get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                System.out.println("Topic deleted: " + name);
            }
        }
    }

    static class KfkProducer {
        Producer<String, String> producer;

        KfkProducer(String host, int port) {
            Properties props = new Properties();
            props.put("bootstrap.servers", String.format("%s:%d", host, port));
//            props.put("linger.ms", 1);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(props);
        }

        void close() {
            producer.close();
        }

        RecordMetadata send(String topic, String key, String value) {
            Future<RecordMetadata> result = producer.send(new ProducerRecord<>(topic, key, value));
            RecordMetadata meta = null;
            try {
                meta = result.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return meta;
        }
    }

    static class KfkConsumer {
        KafkaConsumer<String, String> consumer;

        KfkConsumer(String host, int port, List<String> topics) {
            Properties props = new Properties();
            props.put("bootstrap.servers", String.format("%s:%d", host, port));
            props.put("group.id", "group01");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(topics);
        }

        void close() {
            consumer.close();
        }

        List<List<Object>> poll(int num) {
            List<List<Object>> result = new ArrayList<>();
            while (result.size() < num) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    result.add(Arrays.asList(record.offset(), record.key(), record.value()));
                }
            }
            return result;
        }
    }

    public static void testProducer(String host, int port) {
        KfkProducer producer = new KfkProducer(host, port);
        for (int i = 0; i < 5; i ++) {
            System.out.println(producer.send("test", "" + (i % 5), Integer.toString(i)));
        }
        producer.close();
    }

    public static void testConsumer(String host, int port) {
        KfkConsumer consumer = new KfkConsumer(host, port, Arrays.asList("test"));
        List<List<Object>> records = consumer.poll(5);
        for (List<Object> record: records) {
            System.out.printf("offset = %d, key = %s, value = %s%n",
                    (long)(record.get(0)), record.get(1), record.get(2));
        }
        consumer.close();
    }

    public static void main(String... argv) throws ExecutionException, InterruptedException {
        String host = "192.168.55.12";
        int port = 9092;

        KfkAdmin admin = new KfkAdmin(host, port);
//        admin.createTopic("test", 1, 1);
        System.out.println(admin.topics());
        System.out.println(admin.topicDescription("test"));
        testProducer(host, port);
        testConsumer(host, port);
//        admin.deleteTopic("test");
    }
}
