package de.tu_berlin.kinesistest;

import java.util.Random;

/**
 * This class creates random messages of sizes small,
 * medium and large for the ThroughputTest.
 * It also returns the topics available for the test.
 *
 */
public class MessageCreator {

	private static int sizeSmall = 5;
	private static int sizeMedium = 15;
	private static int sizeLarge = 60;
	
	private Random random;
	/**
	 * creates a new message creator
	 */
	MessageCreator() {
	    random = new Random();
		
	}

	/**
	 * returns a random small message
	 * @return a random small message
	 */
	private String getMsgSmall() {
		byte[] r = new byte[sizeSmall * 1024]; 
		random.nextBytes(r);
		String msg = new String(r);
		return msg;
	}
	/**
	 * returns a random medium message
	 * @return random medium message
	 */
	private String getMsgMedium() {
		byte[] r = new byte[sizeMedium * 1024]; 
		random.nextBytes(r);
		String msg = new String(r);
		return msg;
	}
	/**
	 * returns a random large message
	 * @return a random large message
	 */
	private String getMsgLarge() {
		byte[] r = new byte[sizeLarge * 1024]; 
		random.nextBytes(r);
		String msg = new String(r);
		return msg;
	}
	

	/**
	 * returns a random message.
	 * the size is chosen randomly according to a uniform distribution
	 * (80% small, 15% medium, 5% large)
	 * 
	 * @return a random message
	 */
	public String getRandomMessage() {
		float r = random.nextFloat();
		if(r <= .8f) {
			return getMsgSmall();
		} else if(r <= .95f) {
			return getMsgMedium();
		} else {
			return getMsgLarge();
		}
	}

	/**
	 * returns a random topic.
	 * 
	 * @return a random topic
	 */
	public String getRandomTopic() {
		float r = random.nextFloat();
		if(r <= .33f) {
			return "userevent";
		} else if(r <= .66f) {
			return "operational";
		} else {
			return "advertising";
		}
	}
	/**
	 * returns all topics available
	 * @return all topics 
	 */
	public static String[] getAllTopics() {
		String[] topics =  {"userevent", "operational", "advertising"};
		return topics;
	}
}
