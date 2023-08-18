package org.opensource.seq.redis;

import java.util.Optional;

import org.opensource.seq.core.GlobalSeqPo;
import org.opensource.seq.core.GlobalSeqRepository;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPooled;

/**
 * 全局序列redis存储实现
 * 
 * @author wutianbiao
 * @date 2022-04-20
 */
@Slf4j
public class PooledGlobalSeqRepositoryImpl implements GlobalSeqRepository {
    
    /**
     * 全局序列在jimdb中的前缀
     */
    @Setter
    @Getter
    private String seqPrefix = "sequence:";

    /**
     * redis连接
     */
    private JedisPooled cluster;

    /**
     * 构造方法
     * @param cluster
     */
    public PooledGlobalSeqRepositoryImpl (JedisPooled cluster) {
        this.cluster = cluster;
    }

    @Override
    public int createSeq(GlobalSeqPo po) {
        long result = cluster.setnx(seqPrefix + po.getSeqName(), po.getCurrentValue().toString());
        log.info("创建序列:{}", result);
        return (int)result;
    }

    @Override
    public Optional<GlobalSeqPo> loadSeq(String seqName) {
        String result = cluster.get(this.seqPrefix +  seqName);
        log.info("加载序列:{}", result);
        if(result == null){
            return Optional.empty();
        }
        return Optional.of(new GlobalSeqPo(seqName, Long.parseLong(result)));
    }

    @Override
    public Optional<GlobalSeqPo> lockSeq(String seqName, long step, long old) {
        Long result = cluster.incrBy(this.seqPrefix + seqName, step);
        log.info("锁定序列成功:{}", result);
        return Optional.of(new GlobalSeqPo(seqName, result));
    }
}
