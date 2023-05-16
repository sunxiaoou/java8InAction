package lambdasinaction.tmp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Kafka {
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

    public static void main(String... argv) {
        KfkProducer producer = new KfkProducer("192.168.55.250", 9092);
        for (int i = 0; i < 5; i ++) {
            System.out.println(producer.send("test", "" + (i % 5), Integer.toString(i)));
        }
        producer.close();

        KfkConsumer consumer = new KfkConsumer("192.168.55.250", 9092, Arrays.asList("test"));
        List<List<Object>> records = consumer.poll(5);
        for (List<Object> record: records) {
            System.out.printf("offset = %d, key = %s, value = %s%n",
                    (long)(record.get(0)), record.get(1), record.get(2));
        }
        consumer.close();
    }
}
