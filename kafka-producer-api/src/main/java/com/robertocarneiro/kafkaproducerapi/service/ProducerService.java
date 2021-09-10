package com.robertocarneiro.kafkaproducerapi.service;

import com.robertocarneiro.kafkaproducerapi.dto.TestDTO;

public interface ProducerService {

    TestDTO producerDTO();

    Long producerLong();

    String producerString();
}
