package com.jpmc.midascore.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
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
    private final DatabaseConduit databaseConduit;
    private final IncentiveService incentiveService;

    public KafkaConsumerService(DatabaseConduit databaseConduit, IncentiveService incentiveService) {
        this.databaseConduit = databaseConduit;
        this.incentiveService = incentiveService;
    }

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

            //Validate sender
            UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
            if (sender == null){
                logger.warn("Sender not found: {}", transaction.getSenderId());
                return;
            }

            //Validate recipient
            UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());
            if (recipient == null){
                logger.warn("Recipient not found: {}", transaction.getRecipientId());
                return;
            }

            //Validate sender balance
            if (sender.getBalance() < transaction.getAmount()){
                logger.warn("Insufficient funds: {}", transaction);
                return;
            }

            //Use API Incetive
            Incentive incentive = incentiveService.getIncentive(transaction);
            float incentiveAmount = incentive.getAmount();

            //Update amount
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            //Save sender and recipient
            databaseConduit.save(sender);
            databaseConduit.save(recipient);

            //Save transaction record
            TransactionRecord transactionRecord = new TransactionRecord(
                    sender,
                    recipient,
                    transaction.getAmount(),
                    incentiveAmount
            );
            databaseConduit.saveTransaction(transactionRecord);

            logger.info("Transaction processed: {}", transactionRecord);

        } catch (Exception e) {
            logger.error("Error parsing message: {}", payload, e);
        }
    }
}
