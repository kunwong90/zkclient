package org.zookeeper;

import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;

public class CuratorZkLock {

    public static void main(String[] args) {

        CuratorClient curatorClient = new CuratorClient("localhost:2181");
        String lockPaht = "/curator_recipes_lock_path";
        InterProcessMutex lock = new InterProcessMutex(curatorClient.getClient(), lockPaht);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ExecutorService executorService = new ThreadPoolExecutor(30, 30, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>(100));
        for (int i = 0; i < 30; i++) {
            executorService.execute(()-> {
                try {
                    countDownLatch.await();
                    lock.acquire();
                } catch (Exception e) {

                }
                SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss | SSS");
                System.out.println("生成的订单号" + sdf.format(new Date()));
                try {
                    lock.release();
                } catch (Exception e) {

                }
            });
        }

        countDownLatch.countDown();
    }
}
