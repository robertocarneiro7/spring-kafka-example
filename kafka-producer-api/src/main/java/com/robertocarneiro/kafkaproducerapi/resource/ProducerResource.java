package com.robertocarneiro.kafkaproducerapi.resource;

import com.robertocarneiro.kafkaproducerapi.dto.TestDTO;
import com.robertocarneiro.kafkaproducerapi.service.ProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/producer")
@RequiredArgsConstructor
public class ProducerResource {

    private final ProducerService producerService;

    @GetMapping("/dto")
    public TestDTO producerDTO() {
        return producerService.producerDTO();
    }

    @GetMapping("/long")
    public Long producerLong() {
        return producerService.producerLong();
    }

    @GetMapping("/string")
    public String producerString() {
        return producerService.producerString();
    }
}
