package com.robertocarneiro.kafkaconsumerapi.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static com.robertocarneiro.kafkaconsumerapi.util.Util.logConsumer;

@ConditionalOnExpression("${kafka.enable.group-id-earliest:false}")
@Component
@Slf4j
public class StringEarliestConsumer {

    @KafkaListener(
            topics = "${kafka.topic.string}",
            clientIdPrefix = "${kafka.topic.string}-${kafka.group-id.earliest}",
            groupId = "${kafka.group-id.earliest}",
            containerFactory = "stringEarliestKafkaListenerContainerFactory"
    )
    public void receiveEarliest(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            @Header(KafkaHeaders.OFFSET) Long offset) {
        log.info("String earliest received. " + logConsumer(offset, key, payload));
    }

}
