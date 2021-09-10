package com.robertocarneiro.kafkaproducerapi.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    @Value(value = "${kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${kafka.topic.dto}")
    private String topicDTO;

    @Value(value = "${kafka.topic.long}")
    private String topicLong;

    @Value(value = "${kafka.topic.string}")
    private String topicString;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic topicDTO() {
        return new NewTopic(topicDTO, 1, (short) 1);
    }

    @Bean
    public NewTopic topicLong() {
        return new NewTopic(topicLong, 1, (short) 1);
    }

    @Bean
    public NewTopic topicString() {
        return new NewTopic(topicString, 1, (short) 1);
    }
}
