import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * 分布式锁的案例，这个没做抽象（ZkClient显然应当从外界依赖注入），只是说明一下原理 <br />
 * 它的原理比较简单——每次获取锁时，创建一个临时的序列的Node，然后获取当前所有Node，<br />
 * 检查当前Node是否是最小的节点，如果是则认为获得到了锁，如果否，则监听前一个节点，<br />
 * 这里的具体过程是，先获取前一个节点的状态，如果获取到了，就说明前一个节点未被删除，对前一个节点进行监听并阻塞（使用CountDownLatch）<br />
 * 待到前一个节点被删除，停止阻塞，认为获得锁<br />
 * 仅供学习，工程实践应使用Curator<br />
 * CountDownLatch真好用！似乎特别适合把异步操作转换成为同步操作
 */
public class DistributedLock {
    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        String connectString = "zookeeper1.local:2181,zookeeper2.local:2181,zookeeper3.local:2181";
        zkClient = new ZooKeeper(connectString, 20000, evt -> {
            System.out.println("wo chao");
        });
        //countDownLatch.await();
        if (zkClient.exists("/locks", false) == null) {
            try {
                // 这里显然可能抛异常，但这个异常应当忽略
                zkClient.create("/locks", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (Exception ignored) {}
        }

    }

    ZooKeeper zkClient;
    String currentSeq = null;
    public void zkLock() throws InterruptedException, KeeperException {
        if (currentSeq != null)
            throw new RuntimeException("该锁不可重入");
        currentSeq = zkClient.create("/locks/seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        // 一个坑——getChildren里获得的node名称没有前面的/locks/，需要加上去
        List<String> seqs = zkClient.getChildren("/locks", false).stream()
                .map(str -> "/locks/" + str).sorted().collect(Collectors.toList());

        int currentIndex = seqs.indexOf(currentSeq);

        // 最小的节点就是自己
        if (currentIndex == 0) {
            return; // 成功拿到锁
        }

        // 检测前一个节点当前是否存在，如果存在则监听它；如果不存在就返回
        // 这里为什么再次检查上一个节点的存在？因为如果不对上一个状态的节点进行再次检查的话，
        // 可能上一个节点这时候已经被删除了，再次注册Watcher的话什么都不会发生，必然引发无限循环
        // 这里注册的Watcher也有问题，它遇到任何事件都countDown，可事件不一定是delete事件

//        CountDownLatch latch = new CountDownLatch(1);
//        Stat status = zkClient.exists(seqs.get(currentIndex - 1), evt -> {
//            latch.countDown();
//        });
//        if (status == null) {
//            return;
//        }
//        latch.await();

        // 下面的代码可以解决该问题——若不是delete事件，则反复进行监听
        boolean[] flag = {true};
        while (flag[0]) {
            CountDownLatch latch = new CountDownLatch(1);
            Stat sta = zkClient.exists(seqs.get(currentIndex - 1), evt -> {
                if (evt.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
                    flag[0] = false;
                }
                latch.countDown();
            });
            if (sta == null)
                break;
            latch.await();
        }
    }
    public void zkUnlock() throws InterruptedException, KeeperException {
        if (currentSeq == null)
            throw new RuntimeException("未获取到锁时无法解锁");
        zkClient.delete(currentSeq, -1);
        currentSeq = null;
    }
}
