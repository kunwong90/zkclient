package org.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.zookeeper.util.IpUtil;

import java.util.concurrent.TimeUnit;

public class LeaderSelector {

    private static final String SELECT_MASTER_NODE = "/sysName/select/master/lock";

    private LeaderLatch leaderLatch;

    public LeaderSelector(CuratorFramework client, String id) {
        this.leaderLatch = new LeaderLatch(client, SELECT_MASTER_NODE, id);
    }

    public boolean isLeader() {
        return leaderLatch.hasLeadership();
    }

    public void startSelect() {
        try {
            leaderLatch.addListener(new LeaderLatchListener() {
                @Override
                public void isLeader() {
                    System.out.println("is leader");
                }

                @Override
                public void notLeader() {
                    System.out.println("not leader");
                }
            });
            leaderLatch.start();
            leaderLatch.await();
        } catch (Exception e) {

        }
    }

    public String getLeaderId() {
        try {
            return leaderLatch.getLeader().getId();
        } catch (Exception e) {
            return "";
        }

    }
}
