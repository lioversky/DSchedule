package com.taobao.pamirs.schedule.entity;

import java.util.concurrent.atomic.AtomicLong;

/**
 *	ͳ��ִ����Ϣ
 *
 * @author : lihx
 * create date : 2014-11-17
 */
public class StatisticsInfo {

	private AtomicLong fetchDataNum = new AtomicLong(0);//��ȡ����
	private AtomicLong fetchDataCount = new AtomicLong(0);//��ȡ��������
	private AtomicLong dealDataSucess = new AtomicLong(0);//����ɹ���������
	private AtomicLong dealDataFail = new AtomicLong(0);//����ʧ�ܵ�������
	private AtomicLong dealSpendTime = new AtomicLong(0);//�����ܺ�ʱ,û����ͬ�������ܴ���һ�������
	private AtomicLong otherCompareCount = new AtomicLong(0);//����ȽϵĴ���
	
	public void addFetchDataNum(long value){
		this.fetchDataNum.addAndGet(value);
	}
	public void addFetchDataCount(long value){
		this.fetchDataCount.addAndGet(value);
	}
	public void addDealDataSucess(long value){
		this.dealDataSucess.addAndGet(value);
	}
	public void addDealDataFail(long value){
		this.dealDataFail.addAndGet(value);
	}
	public void addDealSpendTime(long value){
		this.dealSpendTime.addAndGet(value);
	}
	public void addOtherCompareCount(long value){
		this.otherCompareCount.addAndGet(value);
	}
    public String getDealDescription(){
    	return "FetchDataCount=" + this.fetchDataCount 
    	  +",FetchDataNum=" + this.fetchDataNum
    	  +",DealDataSucess=" + this.dealDataSucess
    	  +",DealDataFail=" + this.dealDataFail
    	  +",DealSpendTime=" + this.dealSpendTime
    	  +",otherCompareCount=" + this.otherCompareCount;    	  
    }


}
