package com.haufe.umantis.poc;

import com.haufe.umantis.poc.model.BaseMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import java.util.concurrent.CountDownLatch;

/**
 * @author David Espinosa.
 */
public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private CountDownLatch messageLatch = new CountDownLatch(1);

    public Consumer() {
        LOGGER.info("Consumer created.");
    }

    @KafkaListener(id = "id1", topics = "#{kafkaTopicRandom}")
    public void processMessage(BaseMessage baseMessage,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        LOGGER.info("Received message from topic: " + baseMessage + "on partition " + partition);
        messageLatch.countDown();
    }

    public CountDownLatch getLatch() {
        return messageLatch;
    }
}
