package com.junit.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CuratorCURD {
    private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    private CuratorFramework zkClient = null;
    @Before
    public void before() {
        zkClient =
                CuratorFrameworkFactory.newClient(
                        "127.0.0.1:2181",
                        5000,
                        3000,
                        retryPolicy);
        zkClient.start();
    }

    @After
    public void after() {
        zkClient.close();
    }

    @Test
    public void curd() {
        try {
            zkClient.create().withMode(CreateMode.EPHEMERAL).forPath("/zhangfang", "init".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void transaction() {
        try {
            zkClient.inTransaction().check().forPath("/zhangfang")
                    .and()
                    .create().withMode(CreateMode.EPHEMERAL).forPath("/zhangfang1", "data".getBytes())
                    .and()
                    .setData().withVersion(10086).forPath("/zhangfang2", "data2".getBytes())
                    .and()
                    .commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void listener() {
        try {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .forPath("/zk-huey/cnode", "hello".getBytes());
            /**
             * 在注册监听器的时候，如果传入此参数，当事件触发时，逻辑由线程池处理
             */
            ExecutorService pool = Executors.newFixedThreadPool(2);
            /**
             * 监听数据节点的变化情况
             */
            final NodeCache nodeCache = new NodeCache(zkClient, "/zk-huey/cnode", false);
            nodeCache.start(true);
            nodeCache.getListenable().addListener(
                    new NodeCacheListener() {
                        @Override
                        public void nodeChanged() throws Exception {
                            System.out.println("Node data is changed, new data: " +
                                    new String(nodeCache.getCurrentData().getData()));
                        }
                    },
                    pool
            );
            /**
             * 监听子节点的变化情况
             */
            final PathChildrenCache childrenCache = new PathChildrenCache(zkClient, "/zk-huey", true);
            childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            childrenCache.getListenable().addListener(
                    new PathChildrenCacheListener() {
                        @Override
                        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                                throws Exception {
                            switch (event.getType()) {
                                case CHILD_ADDED:
                                    System.out.println("CHILD_ADDED: " + event.getData().getPath());
                                    break;
                                case CHILD_REMOVED:
                                    System.out.println("CHILD_REMOVED: " + event.getData().getPath());
                                    break;
                                case CHILD_UPDATED:
                                    System.out.println("CHILD_UPDATED: " + event.getData().getPath());
                                    break;
                                default:
                                    break;
                            }
                        }
                    },
                    pool
            );
            zkClient.setData().forPath("/zk-huey/cnode", "world".getBytes());
            Thread.sleep(100 * 1000);
            pool.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
