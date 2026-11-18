package com.streams.playground.word_count;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class WordCoundKafkaConfig {
	
	@Value(value = "${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;
	
	@Value(value = "${spring.kafka.streams.application-id}")
	private String applicationId;
	
	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	KafkaStreamsConfiguration kStreamsConfig() {
		
		Map<String, Object> props = new HashMap<>();
		
		
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, 
				Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, 
				Serdes.String().getClass().getName());
		
		return new KafkaStreamsConfiguration(props);
	}
	
	@Bean
	public KStream<String, String> pipeline(StreamsBuilder builder) {
		
		KStream<String, String> source= 
				builder.stream("wordcount-input");
		
		final Pattern pattern = 
				Pattern.compile("\\W+");
		
		KStream<String, String> counts = 
				source.flatMapValues(value -> 
							Arrays.asList(pattern.split(value.toLowerCase())))
				.map((key, value) -> new KeyValue<String, String>(value, value))
				.filter((key, value) -> (!value.equals("the")))
				.groupByKey()
				.count()
				.mapValues(value -> Long.toString(value))
				.toStream();
		
		counts.to("wordcount-output");
			
		return counts;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
