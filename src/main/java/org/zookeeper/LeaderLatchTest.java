package org.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 通过注册监听的方式来判断自己是否成为leader。调用close()方法释放当前领导权。
 * LeaderLatch通过增加了一个ConnectionStateListener监听连接问题。
 * 如果出现SUSPENDED或者LOST，leader会报告自己不再是leader（直到重新建立连接，否则不会有leader）。
 * 如果LOST的连接被重新建立即RECONNECTED，leaderLatch会删除先前的zNode并重新建立zNode
 */
public class LeaderLatchTest {

    private static final String PATH = "/demo/leader";

    public static void main(String[] args) {

        List<LeaderLatch> latchList = new ArrayList<>();
        try {
            CuratorFramework client = getClient();
            for (int i = 0; i < 10; i++) {



                final LeaderLatch leaderLatch = new LeaderLatch(client, PATH, "client#" + i);
                leaderLatch.addListener(new LeaderLatchListener() {
                    @Override
                    public void isLeader() {
                        System.out.println(leaderLatch.getId() +  ":I am leader. I am doing jobs!");
                    }

                    @Override
                    public void notLeader() {
                        System.out.println(leaderLatch.getId() +  ":I am not leader. I will do nothing!");
                    }
                });
                latchList.add(leaderLatch);
                leaderLatch.start();
            }
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {


            for(LeaderLatch leaderLatch : latchList){
                try {
                    System.out.println("is leader = " + leaderLatch.hasLeadership() + ",leader id = " + leaderLatch.getLeader().getId());
                } catch (Exception e) {

                }
                //CloseableUtils.closeQuietly(leaderLatch);
            }


        }
    }

    private static CuratorFramework getClient() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(6000)
                .connectionTimeoutMs(3000)
                .namespace("demo")
                .build();
        client.start();
        return client;
    }
}
