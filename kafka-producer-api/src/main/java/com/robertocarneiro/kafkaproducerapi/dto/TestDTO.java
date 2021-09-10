package com.robertocarneiro.kafkaproducerapi.dto;

import lombok.*;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
public class TestDTO {

    private Long id;
    private String name;
    private LocalDateTime createdAt;
}
