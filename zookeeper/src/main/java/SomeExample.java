import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static com.ibm.dtfj.javacore.parser.j9.section.monitor.MonitorPatternMatchers.lock;


// 使用JUnit能方便初始化和销毁客户端
public class SomeExample {

    // zk服务端的2181端口，不同服务端的地址以英文逗号分割，不能有空格
    // zookeeper在static块里（这谁想得到啊！）会调用一个获取hostname的方法，但该方法奇慢无比
    // 解决方案是在host里加上 127.0.0.1 MY_HOST_NAME, ::1 MY_HOST_NAME
    // 罪魁祸首在org.apache.zookeeper.Environment的61行
    private static final String connectString = "zookeeper1.local:2181,zookeeper2.local:2181,zookeeper3.local:2181";

    private ZooKeeper zkClient;
    @Before
    public void init() throws IOException {

    }

    @Test
    public void create() throws InterruptedException, KeeperException {
        zkClient.create("/test/createByClient", "who am i?".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        List<String> res = zkClient.getChildren("/test", false);
        System.out.println(res);
    }

    @Test
    public void watchChildrenChange() throws InterruptedException, KeeperException {
        // 和控制台的Watcher相同，是一次性的监听
        CountDownLatch latch = new CountDownLatch(1);
        // 这玩意是异步的，使用CountDownLatch转成同步操作
        zkClient.getChildren("/test", evt -> {
            System.out.println(evt);
            latch.countDown();
        });
        latch.await();

    }

    /**
     * 我不知道真实的分布式锁是怎么写的，但肯定不是这样
     */
    @Test
    public void distributedLock() throws InterruptedException {
        // 模拟的"临界区"
        int[] resource = {0};

        Runnable distributedTask = () -> {
            for (int i = 0; i < 1000; i++) {
                while(!acquire());
                // System.out.println("appA get lock");
                // 既然已经加锁，使用非原子操作也是可以的
                resource[0]++;
                while(!release());
            }
        };

        Thread appA = new Thread(distributedTask);
        Thread appB = new Thread(distributedTask);
        Thread appC = new Thread(distributedTask);
        appA.start();
        appB.start();
        appC.start();
        appA.join();
        appB.join();
        appC.join();
        System.out.println(resource[0]);
    }

    // 取得锁，考虑到互斥的写操作只有一个能成功，多个线程（系统）同时获取锁，应当只有一个能成功
    private boolean acquire() {
        try {
            zkClient.create("/test/lock", "who am i?".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return true;
        } catch (Exception e) {
            // e.printStackTrace();
            return false;
        }
    }
    // 释放锁
    private boolean release() {
        try {
            zkClient.delete("/test/lock", -1);
            return true;
        } catch (Exception e) {
            // e.printStackTrace();
            return false;
        }
    }

    /**
     * 测试手写的分布式锁，性能好像还行
     * @throws InterruptedException
     * @throws KeeperException
     */
    @Test
    public void distributedLock2() throws InterruptedException, KeeperException {
        // 模拟的"临界区"
        int[] resource = {0};

        Runnable distributedTask = () -> {
            DistributedLock lock;
            try {
                lock = new DistributedLock();
            } catch (Exception e) { throw new RuntimeException(e); }
            for (int i = 0; i < 1000; i++) {
                try {
                    lock.zkLock();
                } catch (Exception e) { throw new RuntimeException(e); }
                resource[0]++;
                try {
                    lock.zkUnlock();
                } catch (Exception e) { throw new RuntimeException(e); }
            }
        };

        Thread appA = new Thread(distributedTask);
        Thread appB = new Thread(distributedTask);
        Thread appC = new Thread(distributedTask);
        appA.start();
        appB.start();
        appC.start();
        appA.join();
        appB.join();
        appC.join();
        System.out.println(resource[0]);
    }
}
