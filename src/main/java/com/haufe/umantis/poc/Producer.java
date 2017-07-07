package com.haufe.umantis.poc;

import com.haufe.umantis.poc.model.BaseMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * Basic spring kafka producer
 *
 * @author David Espinosa.
 */
public class Producer {

    public static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    @Autowired
    private KafkaTemplate<String, BaseMessage> kafkaTemplate;

    public void send(String topic, BaseMessage baseMessage) {

        ListenableFuture<SendResult<String, BaseMessage>> future = kafkaTemplate.send(topic, baseMessage);
        future.addCallback(new ListenableFutureCallback<SendResult<String, BaseMessage>>() {

            @Override
            public void onSuccess(final SendResult<String, BaseMessage> stringStringSendResult) {
                LOGGER.info("sent baseMessage= " + baseMessage + " with offset= " + stringStringSendResult.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(final Throwable throwable) {
                LOGGER.error("unable to send baseMessage= " + baseMessage, throwable);
            }
        });
    }
}
