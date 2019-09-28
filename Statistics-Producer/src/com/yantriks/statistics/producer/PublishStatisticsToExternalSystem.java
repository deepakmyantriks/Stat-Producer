package com.yantriks.statistics.producer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.json.JSONObject;

import com.yantra.yfc.statistics.interfaces.IStatisticObject;
import com.yantra.yfc.statistics.interfaces.PLTStatisticsConsumer;
import com.yantriks.statistics.properties.KafkaProperties;

/**
 * This class implements PLTStatisticsConsumer interface . So that Statistics
 * data can be sent to an external system instead of Oracle DB
 *
 * @author Yantriks
 */

public class PublishStatisticsToExternalSystem implements PLTStatisticsConsumer {

	//private static final YFCLogCategory logger =
	//YFCLogCategory.instance(PublishStatisticsToExternalSystem.class.getName());

	@Override
	public void consumeStatistics(Collection<IStatisticObject> stats) {

		if (stats.isEmpty()) {
			System.out.println("No data to work with in consumeStatistics");
			return; // No data to work with
		}
		Map<String, List<IStatisticObject>> iResponseTimeStatisticObjectMap = new HashMap<>();
		Map<String, List<IStatisticObject>> iAgentStatisticObjectMap = new HashMap<>();
		// Copy the unmodifiable collection to a modifiable one
		Collection<IStatisticObject> statCol = new ArrayList<>(stats);
		Iterator<IStatisticObject> iter = statCol.iterator();
		while (iter.hasNext()) {
			IStatisticObject statObj = iter.next();
			if ("ResponseTime".equals(statObj.getStatName())) {
				System.out.println("statName is ResponseTime");
				getStatistics(iResponseTimeStatisticObjectMap, iter, statObj);
			}else {
				System.out.println("statName is AGENT");
				getStatistics(iAgentStatisticObjectMap, iter, statObj);
			}
		}
		
		KafkaProd kafka = new KafkaProd(KafkaProperties.TOPIC);
		if(!iResponseTimeStatisticObjectMap.isEmpty()) {
		JSONObject responseJsonObj = new JSONObject(iResponseTimeStatisticObjectMap);
		//responseJsonObj.replace("OffsetVal", getOffset());
		System.out.println("Json obj for response:" + responseJsonObj);
		kafka.sendMsgToKafka(responseJsonObj, kafka);
		}
		
		if(!iAgentStatisticObjectMap.isEmpty()) {
		JSONObject agentJsonObj = new JSONObject(iAgentStatisticObjectMap);
		//agentJsonObj.replace("OffsetVal", getOffset());
		System.out.println("Json obj for agent:" + agentJsonObj);
		kafka.sendMsgToKafka(agentJsonObj, kafka);
		}
		
		System.out.println("End of class");

	}
	
	/*private static String getOffset() {

		// Getting the size value from COP file
		String currentOffsetSize = "120";
		TimeZone tz = TimeZone.getTimeZone("CST");
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMdd");
		format1.setTimeZone(tz);

		double minOfTheHour = cal.get(Calendar.MINUTE);
		double hourOfDay =  cal.get(Calendar.HOUR_OF_DAY)+(minOfTheHour/60) ;

		int sizeForOffset = (24 * 60) / Integer.parseInt(currentOffsetSize);
		double setHour = (double) sizeForOffset / (double) 24;
		int setOffsetOfTheDay = (int) (hourOfDay * setHour) + 1;

		// Preparing the OFFSET value
		StringBuilder sb = new StringBuilder();
		sb.append(String.valueOf(format1.format(cal.getTime())));
		sb.append(setOffsetOfTheDay);
	    System.out.println(sb.toString());
	    return sb.toString();
	} */

	/**
	 * This method is to form the key for Hash Map.
	 * @param iStatisticObjectMap
	 * @param iter
	 * @param statObj
	 */
	private void getStatistics(Map<String, List<IStatisticObject>> iStatisticObjectMap, Iterator<IStatisticObject> iter,
			IStatisticObject statObj) {

		List<IStatisticObject> iStatisticObjectList;
		String sStatObjKey = statObj.getContextName() + statObj.getHostName() + statObj.getServerId()
				+ statObj.getServerName() + statObj.getServiceName() + statObj.getServiceType() + statObj.getStatName();
		
		System.out.println("sStatObjKey is : " +sStatObjKey);

		if (iStatisticObjectMap.containsKey(sStatObjKey)) {

			iStatisticObjectList = iStatisticObjectMap.get(sStatObjKey);
			iStatisticObjectList.add(statObj);
		} else {

			iStatisticObjectList = new ArrayList<>();
			iStatisticObjectList.add(statObj);
			iStatisticObjectMap.put(sStatObjKey, iStatisticObjectList);
		}
		iter.remove();

	}

}