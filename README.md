# 全局序列(global-seq)
序列曾经作为数据库的基础能力，几乎不会有场景需要单独实现；
但在以微服务、分库分表等为特征的大型业务系统中，却变成了不可或缺的基础能力。
在数据进行分库分表之后，原来的数据序列，就不能在分库的环境中保持不重复。

另一方面在业务系统中却很多地方需要使用序列，比如为切割的数据库表生成序列，为任何需要标识的业务生成唯一编码，记录调用点击等唯一标识等。

常见的一些工具都有一些不足之处，比如：
UUID，生成了较长的字符串浪费存储空间，不能生成数值类型。
雪花算法，需要为每一个实例获得一个全局唯一的种子，会带来新的问题，同时生成的序列也有UUID的一些问题。
一些数据库提供的序列，不能在数据库外部单独获取，并且只能在唯一索引上使用。 

本全局序列实现基于以上使用场景，解决全局序列的技术问题，并提供以下便利：
- 简洁、一致的使用方式
- 可扩展的底层实现
- 高性能（本地jvm缓存序列段、并发获取）
- 提供基于数据库、redis、etcd、zookeeper的开箱即用的实现方式
- spring boot starter零配置集成
- 完备的测试
- 更多实现，敬请期待......

## 1. STRUCTURE 项目结构
- global-seq-core: 是全局序列的核心模型，也是使用的入口GlobalSequence所在
- global-seq-db: 基于数据库存储的全局序列实现
- global-seq-jimdb: 基于jimdb存储的全局序列实现
- global-seq-starter: 全局序列的spring boot starter，默认使用global-seq-db的数据库实现
- global-seq-etcd: 基于etcd的全局序列实现
- global-seq-zookeeper: 基于zookeeper的全局序列实现
- global-seq-redis: 基于redis的全局序列实现

## 2. HOWTO 集成使用
## 2.1 快速上手
spring-boot的项目只需简单引入如下maven依赖:
```xml
<dependency>
  <groupId>org.opensource</groupId>
  <artifactId>global-seq-starter</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

因为，默认是数据库实现，所以需要执行如下sql，创建对应的序列表
```sql
-- mysql
create table `global_seq` (
    `id` bigint NOT NULL AUTO_INCREMENT COMMENT '自增id',
    `seq_name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '序列名称',
    `current_value` bigint DEFAULT NULL COMMENT '序列当前值',
    PRIMARY KEY (`id`) USING BTREE,
    UNIQUE KEY `uniq_seqName` (`seq_name`) USING BTREE COMMENT '序列名称唯一不重复'
  ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '全局序列表';
```
```sql
-- sqlite3
create table `global_seq` (
    `id` integer NOT NULL primary key AUTOINCREMENT,
    `seq_name` varchar(50) NOT NULL,
    `current_value` bigint DEFAULT 0
 );
Create index idx_seq_name on global_seq(seq_name);
```

## 2.2 测试
每个模块的测试目录中，都有对应的测试用例。  
也可以在项目中，执行如下单测，来测试序列的功能
```java
import java.util.ArrayList;
import java.util.List;

import org.opensource.seq.core.GlobalSequence;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@Rollback
public class GlobalSequence2Test {

    @Autowired
    private GlobalSequence globalSequence;

    /**
     * 十个线程，并发获取不超过1000的序列
     */
    @Test
    public void testGetSeqNext() throws InterruptedException {
        String seqName = "test_seq_name";
        long threshold = 1_000;

        List<Thread> pool = new ArrayList<>();

        for(int i = 0; i < 10; i++) {
            Thread t = new GetSeqThread(i, seqName, threshold, globalSequence);
            t.start();
            pool.add(t);
        }

        for(Thread t : pool) {
            t.join();
        }

        System.out.println("测试结束");
    }


    static class GetSeqThread extends Thread {
        private int index;
        private String seqName;
        private long threshold;
        private GlobalSequence globalSequence;

        public GetSeqThread(int index, String seqName,long threshold, GlobalSequence globalSequence) {
            this.index = index;
            this.seqName = seqName;
            this.threshold = threshold;
            this.globalSequence = globalSequence;
        }

        @Override
        public void run() {
            super.run();

            long next = 0L;
            while(true) {
                next = globalSequence.next(seqName);
                if(next > threshold) {
                    break;
                }
                System.out.println("线程"+index+"序列值:"+next);
            }
        }
    }
}
```

## 2.3 全局序列的配置
全局序列默认支持的所有配置，如下yml格式所示: 
```yml
---
global-sequence:
  default-table: global_seq  # 数据库序列表的默认名称
  default-step: 100  # 默认每次获取的序列段长度
  max-retry: 30  # 内部锁定库存最大重试次数
  steps:
    ${seq_name}: 
      start: 999  # 为每个序列定义起始序列值
      step: 200  # 为每个序列定义不同的缓存序列段长度
    ${seq_name}:
      start: 1
      step: 500
```
> 自定义仓储实现，可能会有自己的配置

## 2.4 集成自定义存储实现
1. 使用自定义存储实现，需要引用starter和自己的存储实现:
```xml
<dependency>
  <groupId>org.opensource</groupId>
  <artifactId>global-seq-starter</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>org.opensource</groupId>
  <artifactId>global-seq-jimdb</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```
2. 需要在spring容器注册一个自己存储实现的bean
```java
@Configuration
public class GlobalSequenceAutoConfiguration {
    @Bean
    public GlobalSeqRepository dbSeqRepository(Cluster cluster) {
        log.info("创建jimdb SeqRepository");
        return new org.opensource.selling.seq.jimdb.GlobalSeqRepositoryImpl(cluster);
    }
}
```

## 2.5 非spring-boot使用
创建GlobalSequenceImpl对象，仅需要GlobalSeqConfig和GlobalSeqRepository两个入参。

## 3. CUSTOMIZATION 如何扩展实现？
自定义适合自己系统的底层存储，非常简单，仅需以下几个步骤:  
1. maven添加global-seq-core的依赖。
2. 定义GlobalSeqRepository的实现类，实现具体的方法。
3. 在自己的系统中添加GlobalSeqRepository的bean。
> 如果系统已有GlobalSeqRepository的bean，那么global-seq-starter就不会注册基于数据库的repository实现。
整个系统的全局序列存储，就会切换到自定义的底层存储。

## 4. TASK 后续开发任务
- [x] 全局序列核心模型
- [x] 基于数据库的全局序列
- [x] 全局序列的spring boot starter
- [x] 工程内单测
- [x] 基于zookeeper的全局序列
- [x] 基于etcd的全局序列
- [x] 基于redis的全局序列

## 5. DEVELOPER 开发者
- 欢迎参与，贡献有你

## 6. RELEASE 版本记录
- 2022-04-20 完成zookeeper的实现；完成redis实现。
- 2022-04-18 完成ETCD实现的开发，完善已有模块的单测。
- 2022-03-10 1.0-SNAPSHOT发布，完成核心模块，spring boot starter和db、redis的存储实现！

