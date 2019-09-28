package com.yantriks.statistics.producer;

import java.util.Properties;

import org.apache.commons.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sterlingcommerce.baseutil.SCXmlUtil;
import com.yantra.interop.japi.YIFClientFactory;
import com.yantra.ycp.core.YCPContext;
import com.yantra.yfc.util.YFCException;
import com.yantriks.statistics.properties.KafkaProperties;

public class KafkaProd {
	
	//To add kafka properties
	
	
	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	
	  public KafkaProd(String topic) {
	  System.out.println("Producer constructor"); 
	  Properties props = new Properties(); 
	  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
	  //props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
	  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class.getName());
	  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName()); 
	  producer = new KafkaProducer<>(props);
	  this.topic = topic;
	  
	  }
	  
		//Send message to kafka topic
		
	public void sendMsgToKafka(JSONObject jsson, KafkaProd kafka) {

		try {
			kafka.producer.send(new ProducerRecord<>(kafka.topic, 1, jsson.toString())).get();
			System.out.println("SynchronousProducer Completed with success.");
		} catch (Exception e) {
			long startTime= System.currentTimeMillis();
			System.out.println("Exception while sending message to kafka::" + e.getMessage());
			try (YCPContext oEnv = new YCPContext("system", "system")) {
				oEnv.setSystemCall(true);
				Document inForAPI = SCXmlUtil.createDocument("CreateAsyncRequest");
				Element inElemForAPI = inForAPI.getDocumentElement();
				Element apiElem = SCXmlUtil.createChild(inElemForAPI, "API");
				Element inputElem = SCXmlUtil.createChild(apiElem, "Input");
				apiElem.setAttribute("IsService", "Y");
				apiElem.setAttribute("Name", "SendMessagesToKafka");
				Element sendToKakfaElem = SCXmlUtil.createChild(inputElem, "SendToKafka");
				sendToKakfaElem.setAttribute("JSONObject", jsson.toString());
				sendToKakfaElem.setAttribute("KafkaTopicName", "order_stat");
				System.out.println(
						"*** Input XML for the invoking createAsynRequestAPI is ***" + SCXmlUtil.getString(inForAPI));
				YIFClientFactory.getInstance().getApi().invoke(oEnv, "createAsyncRequest", inForAPI);
				oEnv.commit();
				long endTime=System.currentTimeMillis();
				System.out.println("Total Time taken by createAsynRequest API" + (startTime - endTime) + " milliseconds");
				System.out.println("Publish to kafka ends");	
			} catch (Exception ex) {
				System.out.println("Exception Occured while calling createAsynRequest API" + e.getMessage());
				throw new YFCException(ex);
			}
		}
	}
}
