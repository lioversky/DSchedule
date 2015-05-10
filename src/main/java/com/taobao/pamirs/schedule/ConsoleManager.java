package com.taobao.pamirs.schedule;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.datamanager.IScheduleDataManager;
import com.taobao.pamirs.schedule.strategy.TBScheduleFactory;
import com.taobao.pamirs.schedule.zk.ScheduleFactoryDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ScheduleStrategyDataManager4ZK;
import com.taobao.pamirs.schedule.zk.ZKManager;

public class ConsoleManager {	
	protected static transient Logger log = LoggerFactory.getLogger(ConsoleManager.class);

	public final static String configFile =// System.getProperty("user.dir") + File.separator +
			 "E:\\quartz\\tbschedule\\src\\main\\resources\\pamirsScheduleConfig.properties";

	private static TBScheduleFactory scheduleManagerFactory;	
    
	public static boolean isInitial() throws Exception{
		return scheduleManagerFactory != null;
	}
	public static boolean  initial() throws Exception{
		if(scheduleManagerFactory != null){
			return true;
		}
		File file = new File(configFile);
		scheduleManagerFactory = new TBScheduleFactory();
		scheduleManagerFactory.start = false;
		
		if(file.exists() == true){
			//Console不启动调度能力
			Properties p = new Properties();
			FileReader reader = new FileReader(file);
			p.load(reader);
			reader.close();
			scheduleManagerFactory.init(p);
			log.info("加载Schedule配置文件：" +configFile );
			return true;
		}else{
			return false;
		}
	}	
	
	/*public static void main(String[] args) {
		try {
			ConsoleManager.initial();
			Thread.sleep(5000);
			List<ScheduleTaskType> taskTypes = ConsoleManager.getScheduleDataManager().getAllTaskTypeBaseInfo();
			String managerFactoryUUID = "192.168.0.118$lihx-PC$2EDAC2EF5F454A1BB838443919F5610A$0000000001";
			List<ScheduleServer> serverList =ConsoleManager.getScheduleDataManager().selectScheduleServerByManagerFactoryUUID(managerFactoryUUID);
			
			List<ManagerFactoryInfo> list =  ConsoleManager.getScheduleStrategyManager().loadAllManagerFactoryInfo();
			
			List<ScheduleStrategy> scheduleStrategyList =  ConsoleManager.getScheduleStrategyManager().loadAllScheduleStrategy();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}*/
	public static TBScheduleFactory getScheduleManagerFactory() throws Exception {
		if(isInitial() == false){
			initial();
		}
		return scheduleManagerFactory;
	}
	public static IScheduleDataManager getScheduleDataManager() throws Exception{
		if(isInitial() == false){
			initial();
		}
		return scheduleManagerFactory.getScheduleDataManager();
	}
	public static ScheduleStrategyDataManager4ZK getScheduleStrategyManager() throws Exception{
		if(isInitial() == false){
			initial();
		}
		return scheduleManagerFactory.getScheduleStrategyManager();
	}
	
	public static ScheduleFactoryDataManager4ZK getScheduleFactoryManager() throws Exception{
		if(isInitial() == false){
			initial();
		}
		return scheduleManagerFactory.getScheduleFactoryManager();
	}
    public static Properties loadConfig() throws IOException{
    	File file = new File(configFile);
    	Properties properties;
		if(file.exists() == false){
			properties = ZKManager.createProperties();
        }else{
        	properties = new Properties();
        	FileReader reader = new FileReader(file);
        	properties.load(reader);
			reader.close();
		}
		return properties;
    }
	public static void saveConfigInfo(Properties p) throws Exception {
		try {
			FileWriter writer = new FileWriter(configFile);
			p.store(writer, "");
			writer.close();
		} catch (Exception ex) {
			throw new Exception("不能写入配置信息到文件：" + configFile,ex);
		}
			if(scheduleManagerFactory == null){
				initial();
			}else{
				scheduleManagerFactory.reInit(p);
			}
	}
	public static void setScheduleManagerFactory(
			TBScheduleFactory scheduleManagerFactory) {
		ConsoleManager.scheduleManagerFactory = scheduleManagerFactory;
	}
	
}
