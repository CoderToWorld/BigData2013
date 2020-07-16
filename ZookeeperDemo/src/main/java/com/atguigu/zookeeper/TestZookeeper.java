package com.atguigu.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author suyso
 * @create 2020-04-21 15:46
 */
public class TestZookeeper {
    private String connectionString = "hadoop103:2181,hadoop104:2181,hadoop105:2181";
    private int sessionTimeout = 10000;
    private ZooKeeper zkClient;

    /**
     * 创建节点
     *
     * @throws IOException
     */
    @Test
    public void testCreateNode() throws KeeperException, InterruptedException {
        String s = zkClient.create("/sanguo/java",
                "java is No.1".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println("s" + s);
    }

    /**
     * 获取子节点
     */
    @Test
    public void testChildNode() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //重新获得新的子节点
            }
        });
        for (String child : children) {
            System.out.println(child);
        }
        //让线程不结束
        Thread.sleep(Long.MAX_VALUE);
    }


    /**
     * 获取节点内容，设置节点内容
     * @throws IOException
     */
    @Test
    public void testNodeData() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/sanguo", false);
        if(stat == null){
            System.out.println("指定节点不存在");
        }else {
            byte[] data = zkClient.getData("/sanguo", false, stat);
            System.out.println("data:" + new String(data));
        }
        //设置节点内容，如果版本设置为-1，则忽略版本的限制。
        zkClient.setData("/sanguo","sanguoyanyi".getBytes(),stat.getVersion());//乐观锁
    }

    /**
     * 删除
     * @throws IOException
     */
    @Test
    public void testDeleteNode() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/sanguo/java", false);
        if(stat == null ){
            System.out.println("要删除的节点不存在");
        }else{
            zkClient.delete("/sanguo/java",stat.getVersion());
        }
    }

















    @Before
    public void before() throws IOException {
        zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    @After
    public void after() throws InterruptedException {
        zkClient.close();
    }


    @Test
    public void testZkClient() throws IOException, InterruptedException {
        //获取zookeeper客户端连接对象
        String connectionString = "hadoop103:2181,hadoop104:2181,hadoop105:2181";

        //超时时间设置
        //minSessionTimeout=4000
        //maxSessionTimeout=40000
        int sessionTimeout = 10000;
        ZooKeeper zkClient = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            //回调方法，当watcher对象监听到感兴趣的事情后，会调用process方法
            //在process方法中做出相应的处理

            //WatchEvent   : 事件对象，封装了所有发生的事
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
        //操作
        System.out.println("zkClient" + zkClient);
        //关闭
        zkClient.close();
    }
}
