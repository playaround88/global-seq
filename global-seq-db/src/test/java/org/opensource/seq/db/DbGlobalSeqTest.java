package org.opensource.seq.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensource.seq.core.GlobalSeqConfig;
import org.opensource.seq.core.GlobalSequence;
import org.opensource.seq.core.GlobalSequenceImpl;
import org.opensource.seq.core.SeqConfig;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.sqlite.SQLiteDataSource;

import lombok.extern.slf4j.Slf4j;

import org.sqlite.SQLiteConfig;


/**
 * 单元测试
 * 
 * @author wutianbiao
 * @date 2022-04-16
 */
@Slf4j
public class DbGlobalSeqTest {
    private static GlobalSequence globalSequence;

    @BeforeAll
    public static void setUp() {
        GlobalSeqConfig config = new GlobalSeqConfig();
        config.setDefaultTable("hishop_global_seq");
        SeqConfig seqConfig = new SeqConfig();
        seqConfig.setStart(10L);
        seqConfig.setStep(50L);
        Map<String, SeqConfig> seqs = new HashMap<>();
        seqs.put("test_seq_name", seqConfig);
        config.setSeq(seqs);

        // DataSource
        String dbPath = DbGlobalSeqTest.class.getResource("/testdb.sqlite").getPath();
        log.info("数据库文件路径:{}", dbPath);
        SQLiteConfig sqlConfig = new SQLiteConfig();
        SQLiteDataSource datasource = new SQLiteDataSource(sqlConfig);
        datasource.setUrl("jdbc:sqlite:" + dbPath);

        GlobalSeqRepositoryImpl repository = new GlobalSeqRepositoryImpl(datasource, config.getDefaultTable());

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
