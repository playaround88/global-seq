package org.opensource.seq.core;

import java.util.Optional;

/**
 * 全局序列的持久化仓库实现
 *
 * 核心功能是
 * 1. 创建序列
 * 1. 加载持久层的序列到jvm内存
 * 2. 锁定序列一段值，并持久化
 *
 * 可以根据具体系统扩展实现该仓库
 * 仓促底层可以但不限于：db、redis、zookeeper、etcd等
 *
 * @author wutianbiao
 * @date 2022-03-07
 */
public interface GlobalSeqRepository {

    /**
     * 创建一个序列，创建成功返回1
     *
     * @return
     */
    int createSeq(GlobalSeqPo po);

    /**
     * 从持久层加载序列
     *
     * @param seqName
     * @return
     */
    Optional<GlobalSeqPo> loadSeq(String seqName);

    /**
     * 锁定一段(step)序列，当锁定成功时返回“1”
     *
     * @param seqName
     * @param step
     * @param old
     * @return
     */
    Optional<GlobalSeqPo> lockSeq(String seqName, long step, long old);

}
