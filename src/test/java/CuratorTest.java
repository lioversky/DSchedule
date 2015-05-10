import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/**
 * 
 * 
 * @author : lihx create date : 2014-12-8
 */
public class CuratorTest {
	private CuratorFramework zkTools;
	private ConcurrentSkipListSet watchers = new ConcurrentSkipListSet();
	private static Charset charset = Charset.forName("utf-8");

	public CuratorTest() {
		zkTools = CuratorFrameworkFactory.builder().connectString("10.11.21.78:12306").namespace("zk/test").retryPolicy(new RetryNTimes(2000, 20000)).build();
		zkTools.start();

	}

	public void addReconnectionWatcher(final String path, final ZookeeperWatcherType watcherType, final CuratorWatcher watcher) {
		synchronized (this) {
			if (!watchers.contains(watcher.toString()))// ��Ҫ����ظ��ļ����¼�
			{
				watchers.add(watcher.toString());
				System.out.println("add new watcher " + watcher);
				zkTools.getConnectionStateListenable().addListener(new ConnectionStateListener() {
					@Override
					public void stateChanged(CuratorFramework client, ConnectionState newState) {
						System.out.println(newState);
						if (newState == ConnectionState.LOST) {// ����session����
							try {
								if (watcherType == ZookeeperWatcherType.EXITS) {
									zkTools.checkExists().usingWatcher(watcher).forPath(path);
								} else if (watcherType == ZookeeperWatcherType.GET_CHILDREN) {
									zkTools.getChildren().usingWatcher(watcher).forPath(path);
								} else if (watcherType == ZookeeperWatcherType.GET_DATA) {
									zkTools.getData().usingWatcher(watcher).forPath(path);
								} else if (watcherType == ZookeeperWatcherType.CREATE_ON_NO_EXITS) {
									// ephemeral���͵Ľڵ�session�����ˣ���Ҫ���´����ڵ㣬����ע������¼���֮������¼��У�
									// �ᴦ��create�¼�����·��ֵ�ָ�����ǰ״̬
									Stat stat = zkTools.checkExists().usingWatcher(watcher).forPath(path);
									if (stat == null) {
										System.err.println("to create");
										zkTools.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(path);
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				});
			}
		}
	}

	public void create() throws Exception {
		zkTools.create()// ����һ��·��
				.creatingParentsIfNeeded()// ���ָ���Ľڵ�ĸ��ڵ㲻���ڣ��ݹ鴴�����ڵ�
				.withMode(CreateMode.PERSISTENT)// �洢���ͣ���ʱ�Ļ��ǳ־õģ�
				.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)// ����Ȩ��
				.forPath("zk/test");// ������·��
	}

	public void put() throws Exception {
		zkTools.// ��·���ڵ㸳ֵ
		setData().forPath("zk/test", "hello world".getBytes(Charset.forName("utf-8")));
	}

	public void get() throws Exception {
		String path = "zk/test";
		ZKWatch watch = new ZKWatch(path);
		byte[] buffer = zkTools.getData().usingWatcher(watch).forPath(path);
		System.out.println(new String(buffer, charset));
		// ���session���ڵļ��
		addReconnectionWatcher(path, ZookeeperWatcherType.GET_DATA, watch);
	}

	public void register() throws Exception {

		String ip = InetAddress.getLocalHost().getHostAddress();
		String registeNode = "zk/register/" + ip;// �ڵ�·��

		byte[] data = "disable".getBytes(charset);// �ڵ�ֵ

		CuratorWatcher watcher = new ZKWatchRegister(registeNode, data); // ����һ��register watcher

		Stat stat = zkTools.checkExists().forPath(registeNode);
		if (stat != null) {
			zkTools.delete().forPath(registeNode);
		}
		zkTools.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(registeNode, data);// ������·����ֵ

		// ��ӵ�session���ڼ���¼���
		addReconnectionWatcher(registeNode, ZookeeperWatcherType.CREATE_ON_NO_EXITS, watcher);
		data = zkTools.getData().usingWatcher(watcher).forPath(registeNode);
		System.out.println("get path form zk : " + registeNode + ":" + new String(data, charset));
	}

	public static void main(String[] args) throws Exception {
		CuratorTest test = new CuratorTest();
		test.get();
		test.register();
		Thread.sleep(10000000000L);

	}

	public class ZKWatch implements CuratorWatcher {
		private final String path;

		public String getPath() {
			return path;
		}
		public ZKWatch(String path) {
			this.path = path;
		}
		@Override
		public void process(WatchedEvent event) throws Exception {
			System.out.println(event.getType());
			if (event.getType() == EventType.NodeDataChanged) {
				byte[] data = zkTools.getData().usingWatcher(this).forPath(path);
				System.out.println(path + ":" + new String(data, Charset.forName("utf-8")));
			}
		}

	}

	public class ZKWatchRegister implements CuratorWatcher {
		private final String path;
		private byte[] value;
		public String getPath() {
			return path;
		}
		public ZKWatchRegister(String path, byte[] value) {
			this.path = path;
			this.value = value;
		}
		@Override
		public void process(WatchedEvent event) throws Exception {
			System.out.println(event.getType());
			if (event.getType() == EventType.NodeDataChanged) {
				// �ڵ����ݸı��ˣ���Ҫ��¼�������Ա�session���ں��ܹ��ָ�����ǰ������״̬
				byte[] data = zkTools.getData().usingWatcher(this).forPath(path);
				value = data;
				System.out.println(path + ":" + new String(data, charset));
			} else if (event.getType() == EventType.NodeDeleted) {
				// �ڵ㱻ɾ���ˣ���Ҫ�����µĽڵ�
				System.out.println(path + ":" + path + " has been deleted.");
				Stat stat = zkTools.checkExists().usingWatcher(this).forPath(path);
				if (stat == null) {
					zkTools.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(path);
				}
			} else if (event.getType() == EventType.NodeCreated) {
				// �ڵ㱻����ʱ����Ҫ��Ӽ����¼�����������������session���ں�curator��״̬�������ִ����ģ�
				System.out.println(path + ":" + " has been created!" + "the current data is " + new String(value));
				zkTools.setData().forPath(path, value);
				zkTools.getData().usingWatcher(this).forPath(path);
			}
		}
	}

	public enum ZookeeperWatcherType {
		GET_DATA, GET_CHILDREN, EXITS, CREATE_ON_NO_EXITS
	}
	
	
	
	
	
	
	
	
	
	
	
	
	 static String path = "/curator_pathchildrencache_sample";
	    static CuratorFramework client = CuratorFrameworkFactory.builder()
	            .connectString("domain1.book.zookeeper:2181")
	            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
	            .sessionTimeoutMs(5000)
	            .build();
//	    �ӽڵ�仯�ļ���
	    public static void main1(String[] args) throws Exception {
	        client.start();
	        PathChildrenCache cache = new PathChildrenCache(client, path, true);
	        cache.start(StartMode.POST_INITIALIZED_EVENT);
	        cache.getListenable().addListener(new PathChildrenCacheListener() {
	            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
	                switch (event.getType()) {
	                case CHILD_ADDED:
	                    System.out.println("Event : CHILD_ADDED");
	                    break;
	                case CHILD_UPDATED:
	                    System.out.println("Event : CHILD_UPDATED");
	                    break;
	                case CHILD_REMOVED:
	                    System.out.println("Event : CHILD_REMOVED");
	                    break;
	                default:
	                    break;
	                }
	            }
	        });
	        Thread.sleep(Integer.MAX_VALUE);
	    }
//	    �ڵ����ݱ仯�ļ���
	    public static void main2(String[] args) throws Exception {
	        client.start();
	        client.create()
	              .creatingParentsIfNeeded()
	              .withMode(CreateMode.EPHEMERAL)
	              .forPath(path, "init".getBytes());
	        final NodeCache cache = new NodeCache(client,path,false);
	        cache.start(true);
	        cache.getListenable().addListener(new NodeCacheListener() {
	            @Override
	            public void nodeChanged() throws Exception {
	                System.out.println("Node data update, new data: " + 
	                new String(cache.getCurrentData().getData()));
	            }
	        });
	        client.setData().forPath( path, "u".getBytes() );
	        Thread.sleep( Integer.MAX_VALUE );
	    }
}