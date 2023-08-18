package org.opensource.seq.core;

import lombok.Data;

/**
 * 单个序列的配置
 * @author wutianbiao
 * @date 2022-05-25
 */
@Data
public class SeqConfig {
    /**
     * 序列的开始值
     */
    private Long start = 1L;
    /**
     * 序列获取的步长
     */
    private Long step = 100L;
}
