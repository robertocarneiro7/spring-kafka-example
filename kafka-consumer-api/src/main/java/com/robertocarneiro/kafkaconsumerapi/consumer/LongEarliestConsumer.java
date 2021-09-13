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
public class LongEarliestConsumer {

    @KafkaListener(
            topics = "${kafka.topic.long}",
            clientIdPrefix = "${kafka.topic.long}-${kafka.group-id.earliest}",
            groupId = "${kafka.group-id.earliest}",
            containerFactory = "longEarliestKafkaListenerContainerFactory"
    )
    public void receiveEarliest(
            @Payload Long payload,
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            @Header(KafkaHeaders.OFFSET) Long offset) {
        log.info("Long earliest received. " + logConsumer(offset, key, payload));
    }

}
