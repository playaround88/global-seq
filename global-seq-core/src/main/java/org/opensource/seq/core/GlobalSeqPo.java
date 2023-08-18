package org.opensource.seq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 基于表的全局自增序列
 * 
 * @author wutianbiao
 * @date 2022-02-23
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GlobalSeqPo {
    /**
     * 全局序列名称
     */
    private String seqName;
    /**
     * 全局当前序列值
     */
    private Long currentValue;

}
