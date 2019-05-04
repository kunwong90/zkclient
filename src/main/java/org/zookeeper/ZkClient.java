package org.zookeeper;

import org.apache.curator.framework.CuratorFramework;

import java.util.List;

/**
 * ZooKeeper规定所有非叶子节点必须为持久节点
 * @author wangkun
 */
public interface ZkClient {

    /**
     * 创建临时节点或持久化节点
     * @param path
     * @param ephemeral
     */
    void create(String path, boolean ephemeral);

    /**
     * 创建节点并初始化数据
     * @param path
     * @param content
     * @param ephemeral
     */
    void create(String path, String content, boolean ephemeral);

    /**
     * 创建临时有序节点或持久化有序节点
     * @param path
     * @param ephemeral
     */
    void createSequential(String path, boolean ephemeral);

    /**
     * 创建临时有序节点或持久化有序节点并初始化节点数据
     * @param path
     * @param content
     * @param ephemeral
     */
    void createSequential(String path, String content, boolean ephemeral);

    /**
     * 删除节点
     * @param path
     */
    void delete(String path);

    /**
     * 获取节点下的子节点
     * @param path
     * @return
     */
    List<String> getChildren(String path);



    /**
     * 获取节点下的数据内容
     * @param path
     * @return
     */
    String getContent(String path);

    /**
     * 监听指定节点本身变化
     * @param client
     * @param path
     */
    void nodeListener(CuratorFramework client, String path);

    /**
     * 监听指定节点的子节点变化，无法监听二级节点进行事件监听
     * @param client
     * @param path
     */
    void childNodeListener(CuratorFramework client, String path);

    /**
     *创建节点，如果父节点不存在会自动创建父节点，只有非叶子节点都是持久化节点
     * @param path
     */
    void createParentsIfNeeded(String path, boolean ephemeral);
}
