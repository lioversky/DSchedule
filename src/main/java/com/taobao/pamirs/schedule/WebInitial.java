package com.taobao.pamirs.schedule;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import com.taobao.pamirs.schedule.strategy.TBSchedule;

public class WebInitial extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public void init() throws ServletException {
		super.init();
		try {
//			ConsoleManager.initial();
			Thread.sleep(2000);
			TBSchedule.getManagerIntance().initialData();
		} catch (Exception e) {
			throw new ServletException(e);
		}
	}
}
