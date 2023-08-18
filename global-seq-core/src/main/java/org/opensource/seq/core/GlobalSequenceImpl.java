package org.opensource.seq.core;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.extern.slf4j.Slf4j;

/**
 * 基于表的全局序列实现
 *
 * @author wutianbiao
 * @date 2022-02-23
 */
@Slf4j
public class GlobalSequenceImpl implements GlobalSequence {
    /**
     * 序列持久层
     */
    private GlobalSeqRepository repository;
    /**
     * 序列每次从持久层获取的个数
     */
    private GlobalSeqConfig config;
    /**
     * 全局序列本地缓存
     */
    Map<String, SeqCache> seqCachesMap = new HashMap<>();
    /**
     * 缓存读写锁
     */
    ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock rLock = rwl.readLock();
    private final Lock wLock = rwl.writeLock();

    /**
     * 构造器
     */
    public GlobalSequenceImpl (GlobalSeqConfig config, GlobalSeqRepository repository) {
        this.config = config;
        this.repository = repository;
    }

    /**
     * 获取序列的锁定步长
     *
     * @param seqName
     * @return
     */
    private Long getStep(String seqName) {
        if(this.config.getSeq() != null
                && this.config.getSeq().get(seqName) != null) {
            return this.config.getSeq().get(seqName).getStep();
        }
        return this.config.getDefaultStep();
    }

    /**
     * 获取初始值
     * @param seqName
     * @return
     */
    private Long getStart(String seqName) {
        if(this.config.getSeq() != null
                && this.config.getSeq().get(seqName) != null) {
            return this.config.getSeq().get(seqName).getStart();
        }
        return 1L;
    }

    /**
     * 获取序列值
     *
     * @param seqName
     * @return
     */
    @Override
    public long next(String seqName) {
        rLock.lock();
        try {
            // 如果有值直接返回
            SeqCache cache = seqCachesMap.get(seqName);
            if (cache != null) {
                return cache.next();
            }
        } finally {
            rLock.unlock();
        }

        // 初始化cache对象
        return initSeqAndGet(seqName);
    }

    /**
     * 获取当前值
     * @param seqName
     * @return
     */
    @Override
    public long currentValue(String seqName) {
        try {
            this.rLock.lock();
            // 如果有值直接返回
            SeqCache cache = seqCachesMap.get(seqName);
            if (cache != null) {
                return cache.currentValue();
            }
        } finally {
            rLock.unlock();
        }

        // 初始化cache对象
        return initSeqAndGet(seqName);
    }

    /**
     * 初始化序列并获取值
     * @param seqName
     * @return
     */
    private long initSeqAndGet(String seqName) {
        log.info("初始化加载序列:{}", seqName);
        wLock.lock();
        try {
            //加锁后再次判断
            SeqCache cache = seqCachesMap.get(seqName);
            if (cache != null) {
                // 其他线程已创建
                log.info("其他线程已初始化序列{}，直接返回值!", seqName);
                return cache.next();
            }

            // 创建序列缓存管理对象
            cache = new SeqCache(seqName, getStart(seqName), getStep(seqName), config.getMaxRetry(), repository);
            long next = cache.next();

            // 放入序列缓存
            seqCachesMap.put(seqName, cache);

            return next;
        } finally {
            wLock.unlock();
        }
    }

    /**
     * 单个序列缓存管理对象
     *
     * @author wutianbiao
     * @date 2022-02-24
     */
    private static class SeqCache {
        /**
         * 持久层加载最大重试次数
         */
        private int maxRetry = 30;

        /**
         * 序列名称
         */
        private String seqName;
        /**
         * 序列的初始值
         */
        private Long start;
        /**
         * 序列持久层
         */
        private GlobalSeqRepository repository;
        /**
         * 序列当前值，默认从1开始
         */
        private AtomicLong current = new AtomicLong(1L);
        /**
         * 每次获取的序列个数
         */
        private Long step;
        /**
         * 缓存限制
         */
        private volatile Long limit;


        /**
         * 构造函数
         * @param seqName
         * @param step
         * @param repository
         */
        public SeqCache(String seqName, Long start, Long step, int maxRetry, GlobalSeqRepository repository) {
            log.info("创建序列对象{}: {},{},{}", seqName, start, step, maxRetry);
            this.seqName = seqName;
            this.start = start;
            this.step = step;
            this.maxRetry = maxRetry;
            this.repository = repository;
        }

        /**
         * 获取下一个序列值
         * @return
         */
        public long next() {
            if(limit == null) {
                loadOrLock();
            }

            for(int i = 0; i < maxRetry; i++) {
                // 增加一个值并比较，如果小于限制，直接返回
                long next = this.current.incrementAndGet();
                if(next <= limit.longValue()) {
                    return next;
                }

                // 超出重新获取一段
                loadOrLock();
            }

            throw new RuntimeException("超过最大重试次数未能获取序列");
        }

        /**
         * 获取当前值，粗略瞬时值，不可依赖该值
         * @return
         */
        public long currentValue() {
            // 未初始化
            if(this.limit == null) {
                loadOrLock();
            }
            return current.get();
        }

        /**
         * 从持久层载入序列信息
         * 如果没有，创建序列
         */
        private void loadOrLock() {
            log.info("从持久层获取锁定一段序列:{}", seqName);
            synchronized(this){
                // 如果已经初始化，并且当前值小于limit值，直接返回
                if(limit != null && current.get() < limit.longValue()) {
                    log.info("序列已初始化，当前值小于限制值:{} < {}，退出锁定序列段逻辑", current.get(), limit.longValue());
                    return ;
                }

                for(int i = 0; i < maxRetry; i++) {
                    // 查询持久层
                    Optional<GlobalSeqPo> optSeqPo = this.repository.loadSeq(seqName);

                    // 如果没有，创建持久层
                    if(!optSeqPo.isPresent()) {
                        log.info("创建持久层序列:{}", seqName);
                        try {
                            GlobalSeqPo seqPo = new GlobalSeqPo(seqName, this.step + start);
                            int result = this.repository.createSeq(seqPo);
                            if(result == 1) {
                                log.info("持久层序列创建成功:{}", seqName);
                                changeCache(seqPo);
                                break ; // 退出重试锁定
                            }
                        } catch (Exception e) {
                            log.error("创建全局序列持久化异常:{}", e.getMessage(), e);
                            Throwable cause = e.getCause();
                            if (cause instanceof SQLIntegrityConstraintViolationException) {
                                log.info("一致性约束异常，可能持久层序列已创建，忽略上面异常!:{}", seqName);
                                continue ;
                            } else {
                                throw e;
                            }
                        }
                    } else { // 更新锁定
                        GlobalSeqPo seqPo = optSeqPo.get();
                        log.info("持久层加载全局序列对象:{}", seqPo);

                        log.info("更新锁定序列段:{}", seqName);
                        Optional<GlobalSeqPo> optLockResult = this.repository.lockSeq(seqName, step, seqPo.getCurrentValue());
                        if(optLockResult.isPresent()) {
                            GlobalSeqPo lockResult = optLockResult.get();
                            log.info("锁定序列段成功：{}", lockResult);
                            changeCache(lockResult);
                            break ; // 退出重试锁定
                        }
                    }

                }

                log.info("从持久层获取锁定一段序列结束:{}", seqName);
            }
        }

        /**
         * 用持久层数据，修改当前序列值
         * @param seqPo
         */
        private void changeCache(GlobalSeqPo seqPo) {
            this.current.set(seqPo.getCurrentValue() - step);
            this.limit = seqPo.getCurrentValue();
            log.info("序列加载成功{}: {},{}", this.seqName, this.current, this.limit);
        }
    }
}
