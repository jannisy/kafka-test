package de.tu_berlin.kinesistest;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

/**
 * This class creates and manages the Producers for the ThroughputTest
 *
 */
public class ProducerManager {

	KafkaProducer<String, String>[] producers;

	private Random random;
	
	/**
	 * Creates the given number of producers.
	 * @param server the Kafka server
	 * @param numberOfProducers the number of producers
	 */
	@SuppressWarnings("unchecked")
	public ProducerManager(String server, int numberOfProducers) {
	    random = new Random();
		producers = new KafkaProducer[numberOfProducers];
		for(int i = 0; i < numberOfProducers; i++) {
			Properties props = new Properties();
			props.put("bootstrap.servers", server);
	    	 props.put("acks", "all");
	    	 props.put("retries", 0);
	    	 props.put("batch.size", 16384);
	    	 props.put("linger.ms", 1);
	    	 props.put("buffer.memory", 33554432);
	    	 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    	 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	    	 KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
	    	 producers[i] = producer;
		}
	}
	
	/**
	 * returns a random producer
	 * @return a random producer
	 */
	public KafkaProducer<String, String> getProducer() {
		int r = random.nextInt(producers.length);
		return producers[r];
	}

	/**
	 * closes all producers
	 */
	public void closeAll() {
		for(KafkaProducer<?, ?> p : producers) {
			p.close();
		}
	}

	/**
	 * flushes all producers
	 * (i.e. forces each producer to transfer unsent messages)
	 */
	public void flushAll() {
		for(KafkaProducer<?, ?> p : producers) {
			p.flush();
		}
	}
}
