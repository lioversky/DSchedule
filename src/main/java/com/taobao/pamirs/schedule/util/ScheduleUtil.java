package com.taobao.pamirs.schedule.util;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * ���ȴ�������
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
	 * ��strategy��taskName�н�ȡ
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
	 * ��strategy��taskName�н�ȡ
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
	 * ������������
	 * @param serverNum ��ǰ�ܵķ���������
	 * @param taskItemNum ����������
	 * @param maxNumOfOneServer ÿ��server�����������Ŀ
	 * @param maxNum �ܵ���������
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
	 * ��ÿ̨���������ٷֵ�������������Ҫ�����������ÿ̨�������ִ������������ÿ̨�������ԶԴ˲����ٷ������������
	 * @param factoryNums ÿ̨���������ٷֵ�������
	 * @param taskItemNum ����Ҫ���������
	 * @param maxNumOfOneServer ÿ̨�������ִ����
	 * @return ÿ̨�������ԶԴ˲����ٷ������������
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
		// ̰���㷨���䣬�������ȶ����
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
