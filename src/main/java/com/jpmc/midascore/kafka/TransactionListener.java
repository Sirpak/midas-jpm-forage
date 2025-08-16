package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;



// @Component is a Spring annotation that indicates that the class is a Spring-managed bean.
// tell Spring "this is a bean that I want to manage"

@Component
public class TransactionListener {

    // creates a logger (SLF4J)
    private static final Logger log = LoggerFactory.getLogger(TransactionListener.class);

    /**
     * Minimal listener that:
     * - Reads topic name from 'general.kafka-topic'
     * - Deserializes JSON into the provided Transaction class
     * - Does nothing else (per task requirements)
     */
    @KafkaListener(topics = "${general.kafka-topic}")
    public void onMessage(Transaction tx) {
        // Intentionally no business logic for this task.
        // Keep a debug log so you can see it fire if you run locally.
        log.debug("Received transaction: {}", tx);
    }
}


// @KafkaListener is a Spring annotation that indicates that the method is a Kafka listener.
// ${general.kafka-topic} is a Spring expression that reads the value of the kafka-topic property from the application.yml file.
// "look into applicaiton.yml for the property general.kafka-topic"
// then whatever value is there (midas-transactions-test) is the topic that the listener is listening to

// What happens at run time: 
// 1. Spring Boot starts up
// 2. It reads application.yml, finds general.kafka-topic.
// 3. It finds this TransactionListener bean.
// 4. @KafkaListener registers a Kafka consumer bound to the topic from config.
// 5. When a new Kafka message is produced on that topic:
//Spring pulls it off Kafka.
//Uses the configured deserializer to convert JSON → Transaction.
//Calls onMessage(Transaction tx).
//Your logger prints: "Received transaction: Transaction {senderId=1, recipientId=2, amount=100.0}"



