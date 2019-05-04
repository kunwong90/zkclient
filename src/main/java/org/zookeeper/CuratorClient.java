package org.zookeeper;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;

/**
 * @author wangkun
 */
public class CuratorClient extends AbstractZookeeperClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(CuratorClient.class);
    static final Charset CHARSET = Charset.forName("UTF-8");
    private final CuratorFramework client;

    public CuratorClient(String connectString) {
        this(connectString, "");
    }

    public CuratorClient(String connectString, String namespace) {
        //client = CuratorFrameworkFactory.newClient(connectString, 5000, 3000, new ExponentialBackoffRetry(1000, 3));

        client = CuratorFrameworkFactory.builder().connectString(connectString).sessionTimeoutMs(5000).connectionTimeoutMs(3000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3)).defaultData(null).namespace(namespace).build();
        client.getConnectionStateListenable().addListener((CuratorFramework client, ConnectionState state) -> {
            if (state == ConnectionState.LOST) {
                LOGGER.info("client is lost");
            } else if (state == ConnectionState.CONNECTED) {
                LOGGER.info("client is connected");
            } else if (state == ConnectionState.RECONNECTED) {
                LOGGER.info("client is reconnected");
            }
        });
        client.start();

    }

    @Override
    protected void createPersistentSequential(String path) {
        try {
            client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path);
        } catch (KeeperException.NodeExistsException e) {
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void createEphemeralSequential(String path) {
        try {
            client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path);
        } catch (KeeperException.NodeExistsException e) {
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void createPersistentSequential(String path, String data) {
        byte[] dataBytes = data.getBytes(CHARSET);
        try {
            client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path, dataBytes);
        } catch (KeeperException.NodeExistsException e) {
            try {
                client.setData().forPath(path, dataBytes);
            } catch (Exception e1) {
                throw new IllegalStateException(e.getMessage(), e1);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void createEphemeralSequential(String path, String data) {
        byte[] dataBytes = data.getBytes(CHARSET);
        try {
            client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, dataBytes);
        } catch (KeeperException.NodeExistsException e) {
            try {
                client.setData().forPath(path, dataBytes);
            } catch (Exception e1) {
                throw new IllegalStateException(e.getMessage(), e1);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void createPersistent(String path) {
        try {
            client.create().withMode(CreateMode.PERSISTENT).forPath(path);
        } catch (KeeperException.NodeExistsException e) {
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void createEphemeral(String path) {
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
        } catch (KeeperException.NodeExistsException e) {
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void createPersistent(String path, String data) {
        byte[] dataBytes = data.getBytes(CHARSET);
        try {
            client.create().forPath(path, dataBytes);
        } catch (KeeperException.NodeExistsException e) {
            try {
                client.setData().forPath(path, dataBytes);
            } catch (Exception e1) {
                throw new IllegalStateException(e.getMessage(), e1);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void createEphemeral(String path, String data) {
        byte[] dataBytes = data.getBytes(CHARSET);
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path, dataBytes);
        } catch (KeeperException.NodeExistsException e) {
            try {
                client.setData().forPath(path, dataBytes);
            } catch (Exception e1) {
                throw new IllegalStateException(e.getMessage(), e1);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 判断节点是否存在 true:节点存在;false:节点不存在
     *
     * @param path
     * @return
     */
    @Override
    protected boolean checkExists(String path) {
        try {
            return client.checkExists().forPath(path) != null;
        } catch (Exception e) {
        }
        return false;
    }

    @Override
    protected void deletingChildrenIfNeeded(String path) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (KeeperException.NoNodeException e) {
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void doDelete(String path) {
        try {
            client.delete().forPath(path);
        } catch (KeeperException.NoNodeException e) {
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void guaranteedDelete(String path) {
        try {
            client.delete().guaranteed().forPath(path);
        } catch (KeeperException.NoNodeException e) {
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void deleteWithVersion(String path, int version) {
        try {
            client.delete().withVersion(version).forPath(path);
        } catch (KeeperException.NoNodeException e) {
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> getChildren(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }


    @Override
    public String doGetContent(String path) {
        try {
            byte[] dataBytes = client.getData().forPath(path);
            return (dataBytes == null || dataBytes.length == 0) ? null : new String(dataBytes, CHARSET);
        } catch (KeeperException.NoNodeException e) {
            // ignore NoNode Exception.
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public void childNodeListener(CuratorFramework client, String path) {
        /**
         * cacheData 用于配置是否把节点内容缓存起来
         * true:客户端在接收到节点列表变更的同时，也能够获取到节点的数据内容
         * false:无法获取到节点的数据内容
         */
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, path, true);
        try {
            pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            pathChildrenCache.getListenable().addListener((CuratorFramework curatorFramework, PathChildrenCacheEvent event) -> {
                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
                    LOGGER.info("add node, path = {}, data = {}", event.getData().getPath(), new String(event.getData().getData()));
                } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                    LOGGER.info("update node, path = {}, data = {}", event.getData().getPath(), new String(event.getData().getData()));
                } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                    LOGGER.info("remove node, path = {}, data = {}", event.getData().getPath(), new String(event.getData().getData()));
                }
            });
        } catch (Exception e) {

        }

    }

    @Override
    public void nodeListener(CuratorFramework client, String path) {
/**
 * path 代表数据节点的节点路径
 * dataIsCompressed 是否进行数据压缩
 */
        NodeCache nodeCache = new NodeCache(client, path, false);
        try {
            /**
             * 默认false，设置为true，表示NodeCache在第一次启动的时候就会立刻
             * 从ZooKeeper上读取对应节点的数据内容，并保存在Cache中
             */
            nodeCache.start(true);
            nodeCache.getListenable().addListener(() -> {
                LOGGER.info("path = {}", nodeCache.getCurrentData().getPath());
                LOGGER.info("current node data update, new data  is {}", new String(nodeCache.getCurrentData().getData()));
            });
        } catch (Exception e) {

        }
    }

    CuratorFramework getClient() {
        return client;
    }

    @Override
    public void createParentsIfNeeded(String path, boolean ephemeral) {
        try {
            if (ephemeral) {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
            } else {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

    }
}
