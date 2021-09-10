package com.robertocarneiro.kafkaconsumerapi.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.robertocarneiro.kafkaconsumerapi.dto.TestDTO;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
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
@RequiredArgsConstructor
@Slf4j
public class DTOEarliestConsumer {

    private final ObjectMapper objectMapper;

    @SneakyThrows
    @KafkaListener(
            topics = "${kafka.topic.dto}",
            clientIdPrefix = "${kafka.client-id}",
            groupId = "${kafka.group-id.earliest}",
            containerFactory = "stringEarliestKafkaListenerContainerFactory"
    )
    public void receiveEarliest(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            @Header(KafkaHeaders.OFFSET) Long offset) {
        TestDTO dto = objectMapper.readValue(payload, TestDTO.class);
        String dtoStr = objectMapper.writeValueAsString(dto);
        log.info("DTO earliest received. " + logConsumer(offset, key, dtoStr));
    }

}
