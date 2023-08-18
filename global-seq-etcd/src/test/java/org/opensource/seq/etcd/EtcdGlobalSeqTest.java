package org.opensource.seq.etcd;

import java.util.ArrayList;
import java.util.List;

import org.opensource.seq.core.GlobalSeqConfig;
import org.opensource.seq.core.GlobalSequence;
import org.opensource.seq.core.GlobalSequenceImpl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.etcd.jetcd.Client;

/**
 * 单元测试
 * 
 * @author wutianbiao
 * @date 2022-04-16
 */
public class EtcdGlobalSeqTest {
    private static GlobalSequence globalSequence;

    @BeforeAll
    public static void setUp() {
        GlobalSeqConfig config = new GlobalSeqConfig();

        Client client = Client.builder()
                .endpoints("http://localhost:2379")
                .build();
        
        GlobalSeqRepositoryImpl repository = new GlobalSeqRepositoryImpl(client);

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
