package org.opensource.seq.core;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;

/**
 * 全局序列的配置
 * 
 * @author wutianbiao
 * @date 20022-03-07
 */
@Data
public class GlobalSeqConfig {
    /**
     * 每次锁定的序列长度
     */
    private long defaultStep = 100L;

    /**
     * 序列的自定义锁定长度
     */
    private Map<String, SeqConfig> seq = new HashMap<>();

    /**
     * 默认db作为底层存储是的表名称
     */
    private String defaultTable;

    /**
     * 锁定库存最大重试次数
     */
    private int maxRetry = 30;
}
