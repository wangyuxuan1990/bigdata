package com.wangyuxuan.zk.demo1;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

/**
 * @author wangyuxuan
 * @date 2019/12/13 9:56
 * @description zookeeper的javaAPI操作
 */
public class ZkOperate {

    /**
     * 创建永久节点
     */
    @Test
    public void createNode() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        // 获取客户端对象（多个连接地址之间不能有空格，逗号分隔即可）
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 1000, 1000, retryPolicy);
        // 调用start开启客户端操作
        client.start();
        // 通过create来进行创建节点，并且需要指定节点类型
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/app1");
        client.close();
    }

    /**
     * 创建临时节点
     */
    @Test
    public void createNode2() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        // 获取客户端对象（多个连接地址之间不能有空格，逗号分隔即可）
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 1000, 1000, retryPolicy);
        // 调用start开启客户端操作
        client.start();
        // 通过create来进行创建节点，并且需要指定节点类型
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/app2");
        Thread.sleep(5000);
        client.close();
    }

    /**
     * 节点下面添加数据与修改是类似的，一个节点下面会有一个数据，新的数据会覆盖旧的数据
     */
    @Test
    public void updateNodeData() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 1000, 1000, retryPolicy);
        client.start();
        client.setData().forPath("/app1", "hello".getBytes());
        client.close();
    }

    /**
     * 数据查询
     */
    @Test
    public void getNodeData() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 1000, 1000, retryPolicy);
        client.start();
        byte[] bytes = client.getData().forPath("/app1");
        System.out.println(new String(bytes));
        client.close();
    }

    /**
     * zookeeper的watch机制
     */
    @Test
    public void watchNode() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", 1000, 1000, retryPolicy);
        client.start();
        // 设置节点的cache
        TreeCache treeCache = new TreeCache(client, "/app1");
        // 设置监听器和处理过程
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                ChildData data = treeCacheEvent.getData();
                if (data != null) {
                    switch (treeCacheEvent.getType()) {
                        case NODE_ADDED:
                            System.out.println("NODE_ADDED : " + data.getPath() + "  数据:" + new String(data.getData()));
                            break;
                        case NODE_REMOVED:
                            System.out.println("NODE_REMOVED : " + data.getPath() + "  数据:" + new String(data.getData()));
                            break;
                        case NODE_UPDATED:
                            System.out.println("NODE_UPDATED : " + data.getPath() + "  数据:" + new String(data.getData()));
                            break;
                        default:
                            break;
                    }
                } else {
                    System.out.println("data is null : " + treeCacheEvent.getType());
                }
            }
        });
        // 开始监听
        treeCache.start();
        Thread.sleep(3600000);
    }
}
