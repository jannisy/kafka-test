package de.tu_berlin.kinesistest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * A Producer thread
 *
 */
public class ProducerThread extends Thread {

	private int msgCount;
	private long msgSizeTotal;
	private int executionTime;
	private long actualExecutionTime;
	long startingTime;
	ProducerManager producerManager;
	MessageCreator msgCreator;

	/**
	 * creates a new Producer Thread
	 * @param producerManager the producer manager
	 * @param msgCreator the message creator
	 * @param executionTime the execution time in seconds
	 */
	public ProducerThread(ProducerManager producerManager, MessageCreator msgCreator, int executionTime) {
		super();
		this.producerManager = producerManager;
		this.executionTime = executionTime;
		this.msgCreator = msgCreator;
	}

	/**
	 * returns the actual execution time
	 * @return the actual execution time
	 */
	public long getActualExecutionTime() {
		return actualExecutionTime;
	}
	/**
	 * returns the number of messages sent after the execution
	 * @return the number of messages sent after the execution
	 */
	public int getMsgCount() {
		return msgCount;
	}
	/**
	 * returns the amount of data sent after the execution in bytes
	 * @return the amount of data sent after the execution in bytes
	 */
	public long getMsgSizeTotal() {
		return msgSizeTotal;
	}
    public void run() {

    	startingTime =  System.currentTimeMillis();
    	long currentExecutionTime = 0;
    	System.out.println("Starting Producer with exec Time " + executionTime);
	    do {
    		KafkaProducer<String, String> producer = producerManager.getProducer();
    		String msg = msgCreator.getRandomMessage();
    		String topic = msgCreator.getRandomTopic();
    		String key = "KEY-" + Integer.toString(msgCount);
    		int msgSize = msg.length();
    		msgSizeTotal += msgSize;
			ProducerRecord<String, String> p = new ProducerRecord<String, String>(topic, key, msg);
    		
			producer.send(p);
    		currentExecutionTime = getCurrentExecutionTime();
    	} while(currentExecutionTime < executionTime);
    	actualExecutionTime = getCurrentExecutionTimeInMs();
    	System.out.println("Done Producer with actual Time " + actualExecutionTime);
    }
    /**
     * returns the current running time in seconds.
     * 
     * @return the current running time in seconds.
     */
    private long getCurrentExecutionTime() {
    	long currentTime =  System.currentTimeMillis();
    	return (currentTime - startingTime) / 1000;
    }
    /**
     * returns the current execution time in ms
     * @return the current execution time in ms
     */
    private long getCurrentExecutionTimeInMs() {
    	long currentTime =  System.currentTimeMillis();
    	return (currentTime - startingTime);
    	
    }
}
