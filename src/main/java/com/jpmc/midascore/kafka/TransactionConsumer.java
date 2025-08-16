package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);
    private final UserRepository users;

    public TransactionConsumer(UserRepository users) {
        this.users = users;
    }

    /**
     * Consumes JSON messages from the topic set by 'general.kafka-topic'.
     * Spring Kafka auto-deserializes to com.jpmc.midascore.foundation.Transaction.
     */
    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void onMessage(Transaction tx) {
        log.info("Received tx: {}", tx);

        // Optional demo: apply the transfer to user balances (JPA dirty-checking persists on commit)
        var sender = users.findById(tx.getSenderId()).orElse(null);
        var recipient = users.findById(tx.getRecipientId()).orElse(null);
        if (sender != null && recipient != null) {
            sender.setBalance(sender.getBalance() - tx.getAmount());
            recipient.setBalance(recipient.getBalance() + tx.getAmount());
            log.info("Applied transfer {} -> {} amount {}", sender.getId(), recipient.getId(), tx.getAmount());
        }
    }
}

