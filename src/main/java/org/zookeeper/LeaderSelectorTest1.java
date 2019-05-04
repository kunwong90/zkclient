package org.zookeeper;

import org.zookeeper.util.IpUtil;

import java.awt.*;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LeaderSelectorTest1 {

    public static void main(String[] args) {
        ExecutorService executorService = new ThreadPoolExecutor(5, 5, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));
        CuratorClient curatorClient = new CuratorClient("192.168.1.8:2181");
        for (int i = 0; i < 1; i++) {
            executorService.execute(()-> {
                LeaderSelector leaderSelector = new LeaderSelector(curatorClient.getClient(), IpUtil.getInetAddress());
                leaderSelector.startSelect();
                System.out.println("is leader = " + leaderSelector.isLeader() + " ,leader id = " + leaderSelector.getLeaderId());
            });
        }


        try {
            Thread.sleep(500000000);
        } catch (Exception e) {

        }
        executorService.shutdown();
    }
}
