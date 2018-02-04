package com.kafka.v211.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		

		Properties prop = new Properties();

		// Refer Apache Kafka documentation for the properties used.
		prop.setProperty("bootstrap.servers", "localhost:9092");
		prop.setProperty("key.deserializer",StringDeserializer.class.getName());
		prop.setProperty("value.deserializer",StringDeserializer.class.getName());
		
		prop.setProperty("group.id","Consumer-group-01");
		prop.setProperty("enable.auto.commit","true");
		prop.setProperty("auto.commit.interval.ms","1000");
		prop.setProperty("auto.offset.reset","earliest");
		
		org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(prop);
		consumer.subscribe(Arrays.asList("secondtopic"));
		
		while(true){
			ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
			for(ConsumerRecord<String, String> consumerRecord : consumerRecords){
				
				System.out.println("Partion is " +consumerRecord.partition() +
						"Topic Name is " + consumerRecord.topic() + 
						"Offset value is " +  		consumerRecord.offset() +
						"Timestamp is " +  		consumerRecord.timestamp() +
						"The Key is  " + consumerRecord.key() +
						"The value is " + consumerRecord.value()
						);
				
			}
			
		}
		
		

	}

}
