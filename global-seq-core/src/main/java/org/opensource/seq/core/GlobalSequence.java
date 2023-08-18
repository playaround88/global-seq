package org.opensource.seq.core;

/**
 * 基于表的全局序列仓库接口
 * 
 * 管理全系统的所有序列，对外提供简单的使用入口
 * 
 * 使用时直接注入一个GlobalSequence对象
 * 
 * @author wutianbiao
 * @date 2022-02-23
 */
public interface GlobalSequence {
    
    /**
     * 获取序列的下一个值
     * 最佳实践，使用系统自定一个枚举管理所有系统的序列名称
     * 比如{@link GlobalSequenceEnum}
     * 
     * @param seqName
     * @return
     */
    long next(String seqName);

    /**
     * 获取当前值，瞬时值，不可用做判断
     * 
     * @param seqName
     * @return
     */
    long currentValue(String seqName);

}
