package org.opensource.seq.redis;

import java.util.ArrayList;
import java.util.List;

import org.opensource.seq.core.GlobalSeqConfig;
import org.opensource.seq.core.GlobalSeqRepository;
import org.opensource.seq.core.GlobalSequence;
import org.opensource.seq.core.GlobalSequenceImpl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import redis.clients.jedis.JedisPooled;


/**
 * 单元测试
 * 
 * @author wutianbiao
 * @date 2022-04-16
 */
public class RedisGlobalSeqTest {
    private static GlobalSequence globalSequence;

    @BeforeAll
    public static void setUp() {
        GlobalSeqConfig config = new GlobalSeqConfig();
        config.setDefaultStep(50);

        JedisPooled jedis = new JedisPooled("localhost", 6379);
        
        GlobalSeqRepository repository = new PooledGlobalSeqRepositoryImpl(jedis);

        globalSequence = new GlobalSequenceImpl(config, repository);
    }

    /**
     * 十个线程，并发获取不超过1000的序列
     */
    @Test
    public void testGetSeqNext() throws InterruptedException {
        String seqName = "test_seq_name";
        long threshold = 1_000;

        List<Thread> pool = new ArrayList<>();

        for(int i = 0; i < 10; i++) {
            Thread t = new GetSeqThread(i, seqName, threshold, globalSequence);
            t.start();
            pool.add(t);
        }

        for(Thread t : pool) {
            t.join();
        }

        System.out.println("测试结束");
    }


    static class GetSeqThread extends Thread {
        private int index;
        private String seqName;
        private long threshold;
        private GlobalSequence globalSequence;

        public GetSeqThread(int index, String seqName,long threshold, GlobalSequence globalSequence) {
            this.index = index;
            this.seqName = seqName;
            this.threshold = threshold;
            this.globalSequence = globalSequence;
        }

        @Override
        public void run() {
            super.run();

            long next = 0L;
            while(true) {
                next = globalSequence.next(seqName);
                if(next > threshold) {
                    break;
                }
                System.out.println("线程"+index+"序列值:"+next);
            }
        }
    }
}
