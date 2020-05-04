package com.maskibail.data.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Component
public class MessagePublisher implements ApplicationRunner {

    private static KafkaProducer<Long, String> createProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("acks", "all");
        producerProperties.put("retries", 0);
        producerProperties.put("batch.size", 1);
        producerProperties.put("linger.ms", 1);
        producerProperties.put("buffer.memory", 33554432);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(producerProperties);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        KafkaProducer<Long, String> producer = createProducer();
        scenarioOne(producer);
    }

    private void scenarioOne(KafkaProducer<Long, String> producer) throws InterruptedException {
        for (int i = 1; i < 10; i++) {
            producer.send(new ProducerRecord<>("tweets", (long) i, getTweet((long) i)));
        }
        TimeUnit.SECONDS.sleep(30);
        producer.send(new ProducerRecord<>("tweets", 1L, getTweet(1L)));
        TimeUnit.SECONDS.sleep(30);
        producer.send(new ProducerRecord<>("tweets", 1L, getTweet(1L)));
    }

    private String getTweet(Long id) {
        return String.format("%d, user-%d, message from user-%s, %s", id, id, id,
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    }
}
