package com.robertocarneiro.kafkaconsumerapi.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${kafka.client-id}")
	private String clientId;

	private <T> Map<String, Object> consumerConfigs(Class<? extends Deserializer<T>> clazz,
													String autoOffset) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, clazz);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);
		return props;
	}

	private <T> ConsumerFactory<String, T> consumerFactory(Class<? extends Deserializer<T>> clazz,
														   String autoOffset) {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs(clazz, autoOffset));
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Long> longEarliestKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Long> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(LongDeserializer.class, "earliest"));
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Long> longLatestKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Long> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(LongDeserializer.class, "latest"));
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> stringEarliestKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(StringDeserializer.class, "earliest"));
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> stringLatestKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(StringDeserializer.class, "latest"));
		return factory;
	}

}