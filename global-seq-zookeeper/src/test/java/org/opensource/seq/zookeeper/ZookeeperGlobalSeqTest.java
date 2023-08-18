package org.opensource.seq.zookeeper;

import java.util.ArrayList;
import java.util.List;

import org.opensource.seq.core.GlobalSeqConfig;
import org.opensource.seq.core.GlobalSequence;
import org.opensource.seq.core.GlobalSequenceImpl;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * 单元测试
 * 
 * @author wutianbiao
 * @date 2022-04-16
 */
public class ZookeeperGlobalSeqTest {
    private static final String zkUrl = "127.0.0.1:2181";

    private static CuratorFramework client;
    private static GlobalSequence globalSequence;

    @BeforeAll
    public static void setUp() {
        GlobalSeqConfig config = new GlobalSeqConfig();
        config.setDefaultStep(50);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(zkUrl, retryPolicy);
        client.start();

        GlobalSeqRepositoryImpl repository = new GlobalSeqRepositoryImpl(client);

        globalSequence = new GlobalSequenceImpl(config, repository);
    }

    @AfterAll
    public static void destory() {
        client.close();
    }

    /**
     * 十个线程，并发获取不超过1000的序列
     */
    @Test
    public void testGetSeqNext() throws InterruptedException {
        String seqName = "test_seq_name";
        long threshold = 1_000;

        List<Thread> pool = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Thread t = new GetSeqThread(i, seqName, threshold, globalSequence);
            t.start();
            pool.add(t);
        }

        for (Thread t : pool) {
            t.join();
        }

        System.out.println("测试结束");
    }

    static class GetSeqThread extends Thread {
        private int index;
        private String seqName;
        private long threshold;
        private GlobalSequence globalSequence;

        public GetSeqThread(int index, String seqName, long threshold, GlobalSequence globalSequence) {
            this.index = index;
            this.seqName = seqName;
            this.threshold = threshold;
            this.globalSequence = globalSequence;
        }

        @Override
        public void run() {
            super.run();

            long next = 0L;
            while (true) {
                next = globalSequence.next(seqName);
                if (next > threshold) {
                    break;
                }
                System.out.println("线程" + index + "序列值:" + next);
            }
        }
    }
}
