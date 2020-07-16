package com.atguigu.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * @author suyso
 * @create 2020-04-22 9:14
 */
public class Client {
    private String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeOut = 10000 ;
    private ZooKeeper zkClient ;
    private String  parentNode ="/servers";  //约定 /servers在zk中一定存在。
    public static void main(String[] args) throws Exception {
        Client client  = new Client();
        //1. 初始化zk客户端对象
        client.init();
        //2. 获取服务器的信息
        client.readServers();
        //3. 保持线程不结束
        //Thread.sleep(Long.MAX_VALUE);
    }

    private void readServers() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren(parentNode, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //当在线的服务器发生变动后，会执行process方法，我们需要重新获取最新的服务器信息
                try {
                    readServers();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        //将当前在线的服务器信息打印到控制台
        System.out.println("Current On Line Servers : " + children);
    }

    private void init() throws IOException {
        zkClient = new ZooKeeper(connectionString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }
}