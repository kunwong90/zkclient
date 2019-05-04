package org.zookeeper;

/**
 * @author wangkun
 */
public abstract class AbstractZookeeperClient implements ZkClient {
    @Override
    public void create(String path, boolean ephemeral) {
        if (!ephemeral) {
            if (checkExists(path)) {
                return;
            }
        }
        int i = path.lastIndexOf('/');
        if (i > 0) {
            create(path.substring(0, i), false);
        }
        if (ephemeral) {
            createEphemeral(path);
        } else {
            createPersistent(path);
        }
    }

    @Override
    public void delete(String path) {
        doDelete(path);
    }

    @Override
    public void createSequential(String path, boolean ephemeral) {
        if (!ephemeral) {
            if (checkExists(path)) {
                return;
            }
        }
        int i = path.lastIndexOf('/');
        if (i > 0) {
            create(path.substring(0, i), false);
        }
        if (ephemeral) {
            createEphemeralSequential(path);
        } else {
            createPersistentSequential(path);
        }

    }

    @Override
    public void createSequential(String path, String content, boolean ephemeral) {
        if (!ephemeral) {
            if (checkExists(path)) {
                return;
            }
        }
        int i = path.lastIndexOf('/');
        if (i > 0) {
            create(path.substring(0, i), false);
        }
        if (ephemeral) {
            createEphemeralSequential(path, content);
        } else {
            createPersistentSequential(path, content);
        }
    }

    @Override
    public void create(String path, String content, boolean ephemeral) {
        if (checkExists(path)) {
            delete(path);
        }
        int i = path.lastIndexOf('/');
        if (i > 0) {
            create(path.substring(0, i), false);
        }
        if (ephemeral) {
            createEphemeral(path, content);
        } else {
            createPersistent(path, content);
        }
    }

    @Override
    public String getContent(String path) {
        if (!checkExists(path)) {
            return null;
        }
        return doGetContent(path);
    }

    /**
     * 创建持久化节点
     *
     * @param path
     */
    protected abstract void createPersistentSequential(String path);

    /**
     * 创建临时节点
     *
     * @param path
     */
    protected abstract void createEphemeralSequential(String path);

    /**
     * 创建持久化节点，并初始化数据
     *
     * @param path
     * @param data
     */
    protected abstract void createPersistentSequential(String path, String data);

    /**
     * 创建临时节点，并初始化数据
     *
     * @param path
     * @param data
     */
    protected abstract void createEphemeralSequential(String path, String data);


    /**
     * 创建持久化节点
     *
     * @param path
     */
    protected abstract void createPersistent(String path);

    /**
     * 创建临时节点
     *
     * @param path
     */
    protected abstract void createEphemeral(String path);

    /**
     * 创建持久化节点，并初始化数据
     *
     * @param path
     * @param data
     */
    protected abstract void createPersistent(String path, String data);

    /**
     * 创建临时节点，并初始化数据
     *
     * @param path
     * @param data
     */
    protected abstract void createEphemeral(String path, String data);

    /**
     * 判断节点是否存在
     *
     * @param path
     * @return
     */
    protected abstract boolean checkExists(String path);

    /**
     * 删除一个节点，并递归删除其所有子节点
     *
     * @param path
     */
    protected abstract void deletingChildrenIfNeeded(String path);

    /**
     * 删除节点，只能删除叶子节点
     *
     * @param path
     */
    protected abstract void doDelete(String path);

    /**
     * 删除一个节点，强制保证删除，只要客户端会话有效，
     * Curator会在后台持续进行删除操作，直到节点删除成功
     *
     * @param path
     */
    protected abstract void guaranteedDelete(String path);

    /**
     * 删除一个节点，强制指定版本进行删除
     *
     * @param path
     * @param version
     */
    protected abstract void deleteWithVersion(String path, int version);

    protected abstract String doGetContent(String path);
}
