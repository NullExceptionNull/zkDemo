package com.zjh.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class LeaderSelectorClient extends LeaderSelectorListenerAdapter implements Closeable {

    private static final String host = "192.168.2.48:2181";

    private final String name; //模拟当前的进程

    

    private  LeaderSelector leaderSelector;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public LeaderSelectorClient(String name) {
        this.name = name;
    }

    public void setLeaderSelector(LeaderSelector leaderSelector) {
        this.leaderSelector = leaderSelector;
        leaderSelector.autoRequeue();//自动重复参与选举
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        //如果进入当前的方法 意味着当前的进程获得锁 这个方法会被回调 执行完毕后 表示释放leader 权限


        System.out.println(name + " : 现在是leader");

        countDownLatch.await();//阻塞当前的进程 防止挂掉

    }


    public void start() {
        leaderSelector.start(); //开始竞争leader
    }


    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    public static void main(String[] args) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(host)
                .sessionTimeoutMs(50000000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();

        curatorFramework.start();

        LeaderSelectorClient client = new LeaderSelectorClient("ClientA");

        LeaderSelector selector = new LeaderSelector(curatorFramework,"/leader",client);

        client.setLeaderSelector(selector);

        client.start();


    }
}
