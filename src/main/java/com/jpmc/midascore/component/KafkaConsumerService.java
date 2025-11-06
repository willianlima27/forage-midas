package com.jpmc.midascore.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service //This class is a managed component — it automatically creates and injects an instance.
public class KafkaConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    //This transforms the listen() method into a Kafka listener.
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(ConsumerRecord <String, String> record) {
        //captures the message body
        String payload = record.value();

        try{
            //Transforms JSON into Java objects (deserialization)
            Transaction transaction = objectMapper.readValue(payload, Transaction.class);
            //Log messages to the console.
            logger.info("Received message: {}", transaction);

            System.out.println("Received message: " + transaction);
        } catch (Exception e) {
            logger.error("Error parsing message: {}", payload, e);
        }
    }
}
