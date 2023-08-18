package org.opensource.seq;

import javax.sql.DataSource;

import org.opensource.seq.core.GlobalSeqConfig;
import org.opensource.seq.core.GlobalSeqRepository;
import org.opensource.seq.core.GlobalSequence;
import org.opensource.seq.core.GlobalSequenceImpl;
import org.opensource.seq.db.GlobalSeqRepositoryImpl;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

/**
 * 全局序列的spring-boot-starter auto configuration类
 * 
 * @author wutianbiao
 * @date 2022-03-10
 */
@Slf4j
@Configuration
@AutoConfigureAfter({ DataSourceAutoConfiguration.class })
public class GlobalSequenceAutoConfiguration {

    /**
     * 全局序列配置
     * 
     * @return
     */
    @Bean
    @ConfigurationProperties(prefix = "global-sequence")
    public GlobalSeqConfig globalSeqConfig() {
        return new GlobalSeqConfig();
    }

    /**
     * 如果没有自定义序列的repository，默认使用db做底层存储
     * 
     * @param dataSource
     * @return
     */
    @Bean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnMissingBean(GlobalSeqRepository.class)
    public GlobalSeqRepository dbSeqRepository(DataSource dataSource, GlobalSeqConfig config) {
        log.info("创建dbSeqRepository");
        return new GlobalSeqRepositoryImpl(dataSource, config.getDefaultTable());
    }

    /**
     * GlobalSequence使用入口类
     * 
     * @param config
     * @param repository
     * @return
     */
    @Bean
    public GlobalSequence globalSequence(GlobalSeqConfig config, GlobalSeqRepository repository) {
        return new GlobalSequenceImpl(config, repository);
    }

}
