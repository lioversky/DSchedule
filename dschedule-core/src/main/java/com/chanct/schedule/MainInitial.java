package com.chanct.schedule;

import com.chanct.schedule.stragety.TBSchedule;

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
