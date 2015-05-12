package com.chanct.schedule.util;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 调度处理工具类
 * @author xuannan
 *
 */
public class ScheduleUtil {
	public static String OWN_SIGN_BASE ="BASE";

	public static String getLocalHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "";
		}
	}

	public static int getFreeSocketPort() {
		try {
			ServerSocket ss = new ServerSocket(0);
			int freePort = ss.getLocalPort();
			ss.close();
			return freePort;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static String getLocalIP() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (Exception e) {
			return "";
		}
	}

	public static String transferDataToString(Date d){
		SimpleDateFormat DATA_FORMAT_yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return DATA_FORMAT_yyyyMMddHHmmss.format(d);
	}
	public static String transferDataToString(Date d,String formate){
		SimpleDateFormat DATA_FORMAT_yyyyMMddHHmmss = new SimpleDateFormat(formate);
        return DATA_FORMAT_yyyyMMddHHmmss.format(d);
	}
	public static Date transferStringToDate(String d) throws ParseException{
		SimpleDateFormat DATA_FORMAT_yyyyMMddHHmmss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return DATA_FORMAT_yyyyMMddHHmmss.parse(d);
	}
	public static Date transferStringToDate(String d,String formate) throws ParseException{
		SimpleDateFormat FORMAT = new SimpleDateFormat(formate);
        return FORMAT.parse(d);
	}
	public static String getTaskTypeByBaseAndOwnSign(String baseType,String ownSign){
		if(ownSign.equals(OWN_SIGN_BASE) == true){
			return baseType;
		}
		return baseType+"$" + ownSign;
	}
	/**
	 * 从strategy的taskName中截取
	 * @param taskName
	 * @return
	 */
	public static String splitBaseTaskTypeFromTaskType(String taskName){
		 if(taskName.indexOf("$") >=0){
			 return taskName.substring(0,taskName.indexOf("$"));
		 }else{
			 return taskName;
		 }
		 
	}
	/**
	 * 从strategy的taskName中截取
	 * @param taskName
	 * @return
	 */
	public static String splitOwnsignFromTaskType(String taskName){
		 if(taskName.indexOf("$") >=0){
			 return taskName.substring(taskName.indexOf("$")+1);
		 }else{
			 return OWN_SIGN_BASE;
		 }
	}	
	
	/**
	 * 分配任务数量
	 * @param serverNum 当前总的服务器数量
	 * @param taskItemNum 任务项数量
	 * @param maxNumOfOneServer 每个server最大任务项数目
	 * @param maxNum 总的任务数量
	 * @return
	 */
	public static int[] assignTaskNumber(int serverNum,int taskItemNum,int maxNumOfOneServer){
		int[] taskNums = new int[serverNum];
		int numOfSingle = taskItemNum / serverNum;
		int otherNum = taskItemNum % serverNum;
		if (maxNumOfOneServer >0 && numOfSingle >= maxNumOfOneServer) {
			numOfSingle = maxNumOfOneServer;
			otherNum = 0;
		}
		for (int i = 0; i < taskNums.length; i++) {
			if (i < otherNum) {
				taskNums[i] = numOfSingle + 1;
			} else {
				taskNums[i] = numOfSingle;
			}
		}
		return taskNums;
	}
	/**
	 * 按每台机器可以再分的任务数，本次要分配的总数，每台机器最大执行数，来计算每台机器可以对此策略再分配多少任务数
	 * @param factoryNums 每台机器可以再分的任务数
	 * @param taskItemNum 本次要分配的总数
	 * @param maxNumOfOneServer 每台机器最大执行数
	 * @return 每台机器可以对此策略再分配多少任务数
	 */
	public static Map<String, Integer> assignTaskNumber(Map<String, Integer> factoryNums, int taskItemNum, int maxNumOfOneServer) {
		Map<String, Integer> resultNums = new HashMap<String, Integer>();
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>();
		list.addAll(factoryNums.entrySet());
		// ScheduleUtil.ValueComparator vc = new ValueComparator();
		int lastNum = taskItemNum;
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> mp1, Map.Entry<String, Integer> mp2) {
				return mp2.getValue() - mp1.getValue();
			}
		});
		// 贪心算法分配，可最大的先多分配
		for (int i = 0; i < list.size(); i++) {
			int canrun = list.get(i).getValue();
			if (lastNum <= 0)
				break;
			if (lastNum >= canrun) {
				resultNums.put(list.get(i).getKey(), canrun);
				lastNum = lastNum - canrun;
			} else {
				resultNums.put(list.get(i).getKey(), lastNum);
				lastNum = 0;
			}
		}
		return resultNums;
	}
	
	private static class ValueComparator implements Comparator<Map.Entry<String, Integer>>    
    {    
        public int compare(Map.Entry<String, Integer> mp1, Map.Entry<String, Integer> mp2)     
        {    
            return mp1.getValue() - mp2.getValue();    
        }    
    }  
	
	
	private static String printArray(int[] items){
		String s="";
		for(int i=0;i<items.length;i++){
			if(i >0){s = s +",";}
			s = s + items[i];
		}
		return s;
	}
	public static void main(String[] args) {
		int[] a = assignTaskNumber(4,1,0);
		System.out.println(printArray(a));
		System.out.println(printArray((a)));
//		System.out.println(printArray(assignTaskNumber(2,10,0)));
//		System.out.println(printArray(assignTaskNumber(3,10,0)));
//		System.out.println(printArray(assignTaskNumber(4,10,0)));
//		System.out.println(printArray(assignTaskNumber(5,10,0)));
//		System.out.println(printArray(assignTaskNumber(6,10,0)));
//		System.out.println(printArray(assignTaskNumber(7,10,0)));
//		System.out.println(printArray(assignTaskNumber(8,10,0)));		
//		System.out.println(printArray(assignTaskNumber(9,10,0)));
//		System.out.println(printArray(assignTaskNumber(10,10,0)));
//		
//		System.out.println("-----------------");
//		
//		System.out.println(printArray(assignTaskNumber(1,10,3)));
//		System.out.println(printArray(assignTaskNumber(2,10,3)));
//		System.out.println(printArray(assignTaskNumber(3,10,3)));
//		System.out.println(printArray(assignTaskNumber(4,10,3)));
//		System.out.println(printArray(assignTaskNumber(5,10,3)));
//		System.out.println(printArray(assignTaskNumber(6,10,3)));
//		System.out.println(printArray(assignTaskNumber(7,10,3)));
//		System.out.println(printArray(assignTaskNumber(8,10,3)));		
//		System.out.println(printArray(assignTaskNumber(9,10,3)));
//		System.out.println(printArray(assignTaskNumber(10,10,3)));
		
	}
}
