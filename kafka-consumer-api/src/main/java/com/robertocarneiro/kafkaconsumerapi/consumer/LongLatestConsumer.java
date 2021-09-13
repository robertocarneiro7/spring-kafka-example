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
public class LongLatestConsumer {

    @KafkaListener(
            topics = "${kafka.topic.long}",
            clientIdPrefix = "${kafka.topic.long}-${kafka.group-id.latest}",
            groupId = "${kafka.group-id.latest}",
            containerFactory = "longLatestKafkaListenerContainerFactory"
    )
    public void receiveLatest(
            @Payload Long payload,
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            @Header(KafkaHeaders.OFFSET) Long offset) {
        log.info("Long latest received. " + logConsumer(offset, key, payload));
    }

}
