package com.taobao.pamirs.schedule;

import com.taobao.pamirs.schedule.strategy.TBSchedule;

/**
 * 
 * 
 * @author : lihx create date : 2014-12-4
 */
public class MainInitial {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			TBSchedule.getManagerIntance().initialData();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
