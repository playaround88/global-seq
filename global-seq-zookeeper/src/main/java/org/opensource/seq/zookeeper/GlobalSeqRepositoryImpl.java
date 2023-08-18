package org.opensource.seq.zookeeper;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.opensource.seq.core.GlobalSeqPo;
import org.opensource.seq.core.GlobalSeqRepository;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.data.Stat;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于zookeeper的全局序列仓库实现
 *
 * @author wutianbiao
 * @date 2022-03-10
 */
@Slf4j
public class GlobalSeqRepositoryImpl implements GlobalSeqRepository {
    /**
     * 锁最大等待时间
     */
    @Setter
    @Getter
    private long lockTime = 120;
    /**
     * 全局序列在zookeeper中的前缀
     */
    @Setter
    @Getter
    private String seqPrefix = "/sequence/";
    /**
     * 全局序列在zookeeper中的全局锁前缀
     */
    @Setter
    @Getter
    private String lockPrefix = "/sequence/lock/";
    /**
     * Curator实例
     */
    private CuratorFramework client;

    /**
     * 构造方法
     * 
     * @param cluster
     */
    public GlobalSeqRepositoryImpl(CuratorFramework client) {
        this.client = client;
    }

    @Override
    public int createSeq(GlobalSeqPo po) {
        InterProcessMutex lock = new InterProcessMutex(client, lockPrefix + po.getSeqName());
        try {
            if (lock.acquire(lockTime, TimeUnit.SECONDS)) {
                try {
                    Stat seqStat = client.checkExists().forPath(seqPrefix + po.getSeqName());
                    if (seqStat == null) {
                        log.info("序列节点不存在，创建序列:{}", po.getSeqName());
                        client.create().forPath(seqPrefix + po.getSeqName(),
                                po.getCurrentValue().toString().getBytes());
                        return 1;
                    } else {
                        log.info("序列节点已存在，返回0");
                        return 0;
                    }
                } finally {
                    lock.release();
                }
            }
        } catch (Exception e) {
            log.error("加锁失败:{}", e.getMessage(), e);
        }

        return 0;
    }

    @Override
    public Optional<GlobalSeqPo> loadSeq(String seqName) {
        try {
            Stat seqStat = client.checkExists().forPath(seqPrefix + seqName);
            if (seqStat == null) {
                log.info("序列节点不存在:{}", seqName);
                return Optional.empty();
            }

            byte[] valueByte = client.getData().forPath(seqPrefix + seqName);
            Long value = Long.parseLong(new String(valueByte));

            GlobalSeqPo po = new GlobalSeqPo(seqName, value);
            return Optional.of(po);
        } catch (Exception e) {
            log.error("序列加载失败:{}", e.getMessage(), e);
        }
        return Optional.empty();
    }

    @Override
    public Optional<GlobalSeqPo> lockSeq(String seqName, long step, long old) {
        InterProcessMutex lock = new InterProcessMutex(client, lockPrefix + seqName);
        try {
            if (lock.acquire(lockTime, TimeUnit.SECONDS)) {
                try {
                    byte[] valueByte = client.getData().forPath(seqPrefix + seqName);
                    Long value = Long.parseLong(new String(valueByte));

                    Long currentValue = value + step;
                    client.setData().forPath(seqPrefix + seqName, currentValue.toString().getBytes());

                    GlobalSeqPo po = new GlobalSeqPo(seqName, currentValue);
                    return Optional.of(po);
                } finally {
                    lock.release();
                }
            }
        } catch (Exception e) {
            log.error("加锁失败:{}", e.getMessage(), e);
        }
        return Optional.empty();
    }
}
