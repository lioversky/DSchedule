package com.taobao.pamirs.schedule.zk.curator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.zk.PropertiesKeys;
import com.taobao.pamirs.schedule.zk.Version;

/**
 * 
 * 
 * @author : lihx create date : 2014-12-2
 */
public class CuratorManager {
	private static transient Logger log = LoggerFactory.getLogger(CuratorManager.class);
	private CuratorFramework curator;
	private Properties properties;
	private List<ACL> acl = new ArrayList<ACL>();

	public void createCuratorFramework() throws Exception {
		String connectionString = this.properties.getProperty(PropertiesKeys.zkConnectString.toString());
		int connectionTimeoutMs = 10000;
		int sessionTimeoutMs = Integer.parseInt(this.properties.getProperty(PropertiesKeys.zkSessionTimeout.toString()));;
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
		String authString = this.properties.getProperty(PropertiesKeys.userName.toString()) + ":" + this.properties.getProperty(PropertiesKeys.password.toString());

		curator = CuratorFrameworkFactory.builder().connectString(connectionString).retryPolicy(retryPolicy).connectionTimeoutMs(connectionTimeoutMs).sessionTimeoutMs(sessionTimeoutMs)
				.authorization("digest", authString.getBytes()).build();
		curator.start();
		// this.isCheckParentPath =
		// Boolean.parseBoolean(this.properties.getProperty(keys.isCheckParentPath.toString(),"true"));
		acl.clear();
		acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider.generateDigest(authString))));
		acl.add(new ACL(ZooDefs.Perms.READ, Ids.ANYONE_ID_UNSAFE));
	}

	public CuratorManager(Properties aProperties) throws Exception {
		this.properties = aProperties;
		this.createCuratorFramework();
	}
	public void close() {
		curator.close();
	}

	public String getRootPath() {
		return this.properties.getProperty(PropertiesKeys.rootPath.toString());
	}
	public String getConnectStr() {
		return this.properties.getProperty(PropertiesKeys.zkConnectString.toString());
	}
	public CuratorFramework getCurator() {
		return curator;
	}

	/**
	 * 初始化zk信息
	 * 
	 * @throws Exception
	 */
	public void initial() throws Exception {
		// 当zk状态正常后才能调用

		if (curator.checkExists().forPath(this.getRootPath()) == null) {
			curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(acl).forPath(this.getRootPath());

			// 设置版本信息
			curator.setData().withVersion(-1).forPath(this.getRootPath());
		} else {
			// 先校验父亲节点，本身是否已经是schedule的目录

			byte[] value = curator.getData().forPath(this.getRootPath());
			if (value == null) {
				curator.setData().withVersion(-1).forPath(this.getRootPath());
			} else {
				String dataVersion = new String(value);
				if (Version.isCompatible(dataVersion) == false) {
					throw new Exception("TBSchedule程序版本 " + Version.getVersion() + " 不兼容Zookeeper中的数据版本 " + dataVersion);
				}
				log.info("当前的程序版本:" + Version.getVersion() + " 数据版本: " + dataVersion);
			}
		}
	}
	public ZooKeeper getZooKeeper() throws Exception {
		return curator.getZookeeperClient().getZooKeeper();
	}

	public boolean checkZookeeperState() throws Exception {
		return getZooKeeper() != null && getZooKeeper().getState() == States.CONNECTED;
	}
	public List<ACL> getAcl() {
		return acl;
	}
	
	public synchronized void  reConnection() throws Exception{
		this.close();
		this.createCuratorFramework();
	}
}
