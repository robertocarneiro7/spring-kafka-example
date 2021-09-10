package com.robertocarneiro.kafkaconsumerapi.consumer;

import com.robertocarneiro.kafkaconsumerapi.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static com.robertocarneiro.kafkaconsumerapi.util.Util.LOG_MESSAGE_EXCEPTION;
import static com.robertocarneiro.kafkaconsumerapi.util.Util.logConsumer;

@ConditionalOnExpression("${kafka.enable.group-id-exception:false}")
@Component
@Slf4j
public class StringExceptionConsumer {

    @KafkaListener(
            topics = "${kafka.topic.string}",
            clientIdPrefix = "${kafka.client-id}",
            groupId = "${kafka.group-id.exception}",
            containerFactory = "stringEarliestKafkaListenerContainerFactory"
    )
    public void receiveEarliestException(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            @Header(KafkaHeaders.OFFSET) Long offset) {
        BusinessException exception = new BusinessException(LOG_MESSAGE_EXCEPTION);
        log.info("String earliest exception received. " + logConsumer(offset, key, payload), exception);
        throw exception;
    }

}
