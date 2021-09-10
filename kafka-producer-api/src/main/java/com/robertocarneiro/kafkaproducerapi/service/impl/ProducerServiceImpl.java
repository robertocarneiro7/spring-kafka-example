package com.robertocarneiro.kafkaproducerapi.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.robertocarneiro.kafkaproducerapi.dto.TestDTO;
import com.robertocarneiro.kafkaproducerapi.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ProducerServiceImpl implements ProducerService {

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, Long> longKafkaTemplate;
    private final KafkaTemplate<String, String> stringKafkaTemplate;

    @Value(value = "${kafka.topic.dto}")
    private String topicDTO;

    @Value(value = "${kafka.topic.long}")
    private String topicLong;

    @Value(value = "${kafka.topic.string}")
    private String topicString;

    @SneakyThrows
    @Override
    public TestDTO producerDTO() {
        TestDTO testDTO = TestDTO
                .builder()
                .id(new Random().nextLong())
                .name(UUID.randomUUID().toString())
                .createdAt(LocalDateTime.now())
                .build();
        String payload = objectMapper.writeValueAsString(testDTO);
        stringKafkaTemplate.send(topicDTO, payload);
        return testDTO;
    }

    @Override
    public Long producerLong() {
        Long longRandom = new Random().nextLong();
        longKafkaTemplate.send(topicLong, longRandom);
        return longRandom;
    }

    @Override
    public String producerString() {
        String stringRandom = UUID.randomUUID().toString();
        stringKafkaTemplate.send(topicString, stringRandom);
        return stringRandom;
    }
}
