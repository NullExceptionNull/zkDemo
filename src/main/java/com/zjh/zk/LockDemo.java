package com.zjh.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

public class LockDemo {


    private static final String host = "192.168.0.103:2181";


    public static void main(String[] args) throws Exception {

        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(host)
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();

        curatorFramework.start();

        //curatorFramework.create().creatingParentsIfNeeded().forPath("/test");



        //互斥锁
        final InterProcessMutex lock = new InterProcessMutex(curatorFramework, "/locks");

        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                System.out.println(Thread.currentThread().getName() + ": -- 尝试获取锁");

                try {
                    //尝试获取锁
                    lock.acquire();

                    System.out.println(Thread.currentThread().getName() + ": -- 成功获得锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }


                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        lock.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }, "Thread :" + i).start();
        }


    }


}
