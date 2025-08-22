package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * TransactionConsumer
 *
 * Purpose for Task 2:
 * - Subscribe to the Kafka topic whose name comes from application.yml (general.kafka-topic)
 * - Let Spring Kafka deserialize messages from JSON into the provided Transaction class
 * - Do NOT perform any business logic yet (no DB lookups / updates)
 */
@Component // Make this class a Spring bean so it’s discovered by component scanning
public class TransactionConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

    /**
     * @KafkaListener registers this method as a Kafka consumer.
     * topics = "${general.kafka-topic}" means:
     *   - At runtime, Spring reads application.yml -> general.kafka-topic
     *   - The value (e.g., "midas.transactions") is the topic this method listens to
     *
     * The parameter type is Transaction:
     *   - Spring Kafka uses its JSON deserializer to convert the message value bytes into
     *     com.jpmc.midascore.foundation.Transaction before calling this method.
     *
     * For Task 2 we only need to RECEIVE. Logging is optional but handy when running locally.
     */
    @KafkaListener(topics = "${general.kafka-topic}")
    public void onMessage(Transaction tx) {
        // Minimal behavior: confirm we received and deserialized the message.
        // Do NOT touch the database in Task 2.
        log.info("Received transaction: senderId={}, recipientId={}, amount={}",
                tx.getSenderId(), tx.getRecipientId(), tx.getAmount());
        // Set a breakpoint here to capture the first four tx.getAmount() values in the debugger.
    }
}
