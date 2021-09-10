package com.robertocarneiro.kafkaconsumerapi.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static com.robertocarneiro.kafkaconsumerapi.util.Util.logConsumer;

@ConditionalOnExpression("${kafka.enable.group-id-latest:false}")
@Component
@Slf4j
public class StringLatestConsumer {

    @KafkaListener(
            topics = "${kafka.topic.string}",
            clientIdPrefix = "${kafka.client-id}",
            groupId = "${kafka.group-id.latest}",
            containerFactory = "stringLatestKafkaListenerContainerFactory"
    )
    public void receiveLatest(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            @Header(KafkaHeaders.OFFSET) Long offset) {
        log.info("String latest received. " + logConsumer(offset, key, payload));
    }

}
