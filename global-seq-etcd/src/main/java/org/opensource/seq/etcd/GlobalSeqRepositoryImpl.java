package org.opensource.seq.etcd;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.opensource.seq.core.GlobalSeqPo;
import org.opensource.seq.core.GlobalSeqRepository;

import org.apache.commons.collections4.CollectionUtils;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lock.LockResponse;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 全局需求存储，etcd实现
 * 
 * @author wutianbiao
 * @date 2022-04-16
 */
@Slf4j
public class GlobalSeqRepositoryImpl implements GlobalSeqRepository {
    /**
     * 共享锁的存活时长
     */
    @Getter
    @Setter
    private long ttl = 120L;
    /**
     * 序列key的前缀
     */
    @Getter
    @Setter
    private String prefix = "seq_";
    /**
     * 序列锁key的前缀
     */
    @Getter
    @Setter
    private String lockPrefix = "lock_";
    /**
     * etcd client
     */
    private Client client;

    /**
     * 构造函数，etcd client必须
     * 
     * @param client
     */
    public GlobalSeqRepositoryImpl(Client client) {
        this.client = client;
    }

    @Override
    public int createSeq(GlobalSeqPo po) {
        // 全局锁
        String lockName = this.lockPrefix + po.getSeqName();
        ByteSequence lockKey = ByteSequence.from(lockName.getBytes());
        // 序列
        String seqName = this.prefix + po.getSeqName();
        ByteSequence seqKey = ByteSequence.from(seqName.getBytes());

        try {
            // 创建租约
            Lease leaseClient = client.getLeaseClient();
            CompletableFuture<LeaseGrantResponse> grantFutrue = leaseClient.grant(this.ttl);
            LeaseGrantResponse lease = grantFutrue.get();

            // 全局锁
            Lock lockClient = client.getLockClient();
            CompletableFuture<LockResponse> lockFuture = lockClient.lock(lockKey, lease.getID());
            LockResponse lockResponse = lockFuture.get();
            ByteSequence lock = lockResponse.getKey();
            try {
                KV kvClient = client.getKVClient();

                // 查询序列
                CompletableFuture<GetResponse> getFuture = kvClient.get(seqKey);
                GetResponse getResponse = getFuture.get();

                // 已创建，直接返回
                if (getResponse.getCount() > 0) {
                    return 0;
                }

                // 未创建则创建序列
                ByteSequence value = ByteSequence.from(po.getCurrentValue().toString().getBytes());
                CompletableFuture<PutResponse> putFuture = kvClient.put(seqKey, value);
                PutResponse putResponse = putFuture.get();
                log.info("创建序列成功! {}, {}", po.getSeqName(), putResponse);

                // 创建成功返回1
                return 1;
            } finally {
                lockClient.unlock(lock);
            }
        } catch (InterruptedException e) {
            log.error("创建序列异常:{}", e.getMessage(), e);
        } catch (ExecutionException e) {
            log.error("创建序列异常:{}", e.getMessage(), e);
        }

        return 0;
    }

    @Override
    public Optional<GlobalSeqPo> loadSeq(String seqName) {
        // 序列
        String seqStr = this.prefix + seqName;
        ByteSequence seqKey = ByteSequence.from(seqStr.getBytes());

        KV kvClient = client.getKVClient();

        // 查询序列
        CompletableFuture<GetResponse> getFuture = kvClient.get(seqKey);
        try {
            GetResponse getResponse = getFuture.get();
            List<KeyValue> kvs = getResponse.getKvs();
            if(CollectionUtils.isEmpty(kvs)) {
                log.info("未查询到序列:{}", seqName);
                return Optional.empty();
            }

            KeyValue keyValue = kvs.get(0);
            ByteSequence value = keyValue.getValue();
            log.info("成功加载序列:{}", keyValue);

            GlobalSeqPo po = new GlobalSeqPo();
            po.setSeqName(seqName);
            String valStr = new String(value.getBytes());
            po.setCurrentValue(Long.parseLong(valStr));
            return Optional.of(po);
        } catch (InterruptedException e) {
            log.error("加载序列异常:{}", e.getMessage(), e);
        } catch (ExecutionException e) {
            log.error("加载序列异常:{}", e.getMessage(), e);
        }

        return Optional.empty();
    }

    @Override
    public Optional<GlobalSeqPo> lockSeq(String seqName, long step, long old) {
        // 全局锁
        String lockName = this.lockPrefix + seqName;
        ByteSequence lockKey = ByteSequence.from(lockName.getBytes());
        // 序列
        String seqStr = this.prefix + seqName;
        ByteSequence seqKey = ByteSequence.from(seqStr.getBytes());

        try {
            // 创建租约
            Lease leaseClient = client.getLeaseClient();
            CompletableFuture<LeaseGrantResponse> grantFutrue = leaseClient.grant(this.ttl);
            LeaseGrantResponse lease = grantFutrue.get();

            // 全局锁
            Lock lockClient = client.getLockClient();
            CompletableFuture<LockResponse> lockFuture = lockClient.lock(lockKey, lease.getID());
            LockResponse lockResponse = lockFuture.get();
            ByteSequence lock = lockResponse.getKey();
            try {
                KV kvClient = client.getKVClient();

                // 查询序列
                CompletableFuture<GetResponse> getFuture = kvClient.get(seqKey);
                GetResponse getResponse = getFuture.get();

                List<KeyValue> kvs = getResponse.getKvs();
                KeyValue keyValue = kvs.get(0);
                Long cvalue = Long.parseLong(keyValue.getValue().toString());

                // 设置序列
                Long next = cvalue + step;
                ByteSequence value = ByteSequence.from(next.toString().getBytes());
                CompletableFuture<PutResponse> putFuture = kvClient.put(seqKey, value);
                PutResponse putResponse = putFuture.get();
                log.info("锁定序列段成功! {}, {}", seqName, putResponse);

                // 创建成功返回1
                GlobalSeqPo po = new GlobalSeqPo(seqName, next);
                return Optional.of(po);
            } finally {
                lockClient.unlock(lock);
            }
        } catch (InterruptedException e) {
            log.error("锁定序列段异常:{}", e.getMessage(), e);
        } catch (ExecutionException e) {
            log.error("锁定序列段异常:{}", e.getMessage(), e);
        }

        return Optional.empty();
    }

}
