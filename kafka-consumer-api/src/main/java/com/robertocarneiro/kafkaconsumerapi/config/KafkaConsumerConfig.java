package com.robertocarneiro.kafkaconsumerapi.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static com.robertocarneiro.kafkaconsumerapi.util.Util.logException;

@EnableKafka
@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerConfig {

	private final KafkaTemplate<String, Long> longKafkaTemplate;
	private final KafkaTemplate<String, String> stringKafkaTemplate;

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${kafka.client-id}")
	private String clientId;

	@Value("${kafka.topic.dlt.dto}")
	private String topicDltDTO;

	@Value("${kafka.topic.dlt.long}")
	private String topicDltLong;

	@Value("${kafka.topic.dlt.string}")
	private String topicDltString;

	private static final int PARTITION = -1;
	private static final long INTERVAL = 0;
	private static final long MAX_ATTEMPTS = 2L;
	private static final String EARLIEST = "earliest";
	private static final String LATEST = "latest";

	private <T> Map<String, Object> consumerConfigs(Class<? extends Deserializer<T>> clazz,
													String autoOffset) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, clazz);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);
		return props;
	}

	private <T> ConsumerFactory<String, T> consumerFactory(Class<? extends Deserializer<T>> clazz,
														   String autoOffset) {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs(clazz, autoOffset));
	}

	private <T> SeekToCurrentErrorHandler createErrorHandler(KafkaTemplate<String, T> kafkaTemplate,
															 String topicDlt) {
		BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver =
				(record, ex) -> {
					log.error(logException(record.topic(), topicDlt));
					return new TopicPartition(topicDlt, PARTITION);
				};
		DeadLetterPublishingRecoverer dlPublishingRecoverer =
				new DeadLetterPublishingRecoverer(kafkaTemplate, destinationResolver);
		FixedBackOff fixedBackOff = new FixedBackOff(INTERVAL, MAX_ATTEMPTS);
		return new SeekToCurrentErrorHandler(dlPublishingRecoverer, fixedBackOff);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Long> longEarliestKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Long> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(LongDeserializer.class, EARLIEST));
		factory.setErrorHandler(createErrorHandler(longKafkaTemplate, topicDltLong));
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Long> longLatestKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Long> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(LongDeserializer.class, LATEST));
		factory.setErrorHandler(createErrorHandler(longKafkaTemplate, topicDltLong));
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> stringEarliestKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(StringDeserializer.class, EARLIEST));
		factory.setErrorHandler(createErrorHandler(stringKafkaTemplate, topicDltString));
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> stringLatestKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(StringDeserializer.class, LATEST));
		factory.setErrorHandler(createErrorHandler(stringKafkaTemplate, topicDltString));
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> dtoEarliestKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(StringDeserializer.class, EARLIEST));
		factory.setErrorHandler(createErrorHandler(stringKafkaTemplate, topicDltDTO));
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> dtoLatestKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory(StringDeserializer.class, LATEST));
		factory.setErrorHandler(createErrorHandler(stringKafkaTemplate, topicDltDTO));
		return factory;
	}

}