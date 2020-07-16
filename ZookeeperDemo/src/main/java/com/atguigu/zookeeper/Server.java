package com.atguigu.zookeeper;

/**
 * @author suyso
 * @create 2020-04-22 9:14
 */

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * 当服务器上线后，将当前服务器对应的信息写入到zk中
 */
public class Server {

    private String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeOut = 10000;
    private ZooKeeper zkClient = null ;
    private String  parentNode = "/servers";
    public static void main(String[] args) throws Exception {
        Server server = new Server ();
        //1. 初始化zk客户端对象
        server.init();
        //2. 判断zk中存储服务器信息的Znode是否存在
        server.parentNodeExists();
        //3. 将服务器的信息写入到zk中
        server.writeServer(args);
        //4. 保持线程不结束
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * args中包含两个数据:
     *  1. server的名字
     *  2. server的信息
     * @param args
     */
    private void writeServer(String [] args) throws KeeperException, InterruptedException {
        String s =
                zkClient.create(parentNode + "/" + args[0], args[1].getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("*********** "+ s +"is on line  ************");
    }

    private void parentNodeExists() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists(parentNode, false);
        if(stat == null){
            //创建节点
            zkClient.create(parentNode,"servers".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }
    }

    private void init() throws IOException {
        zkClient = new ZooKeeper(connectionString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }
}