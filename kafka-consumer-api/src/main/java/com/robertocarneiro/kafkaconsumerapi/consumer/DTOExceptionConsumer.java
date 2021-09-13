package com.robertocarneiro.kafkaconsumerapi.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.robertocarneiro.kafkaconsumerapi.dto.TestDTO;
import com.robertocarneiro.kafkaconsumerapi.exception.BusinessException;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
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
@RequiredArgsConstructor
@Slf4j
public class DTOExceptionConsumer {

    private final ObjectMapper objectMapper;

    @SneakyThrows
    @KafkaListener(
            topics = "${kafka.topic.dto}",
            clientIdPrefix = "${kafka.topic.dto}-${kafka.group-id.exception}",
            groupId = "${kafka.group-id.exception}",
            containerFactory = "stringEarliestKafkaListenerContainerFactory"
    )
    public void receiveEarliestException(
            @Payload String payload,
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            @Header(KafkaHeaders.OFFSET) Long offset) {
        TestDTO dto = objectMapper.readValue(payload, TestDTO.class);
        String dtoStr = objectMapper.writeValueAsString(dto);
        BusinessException exception = new BusinessException(LOG_MESSAGE_EXCEPTION);
        log.info("DTO earliest exception received. " + logConsumer(offset, key, dtoStr), exception);
        throw exception;
    }

}
