package org.opensource.seq.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import javax.sql.DataSource;

import org.opensource.seq.core.GlobalSeqPo;
import org.opensource.seq.core.GlobalSeqRepository;

import lombok.extern.slf4j.Slf4j;

/**
 * 基于数据库的，全局序列仓库层实现
 *
 * @author wutianbiao
 * @date 2022-03-07
 */
@Slf4j
public class GlobalSeqRepositoryImpl implements GlobalSeqRepository {

    /**
     * 数据库seqName字段名称
     */
    private static final String SEQ_NAME = "seq_name";
    /**
     * 数据库currentValue字段名称
     */
    private static final String CURRENT_VALUE = "current_value";
    /**
     * 创建序列sql
     */
    private static volatile String CREATE_SQL = "insert into %s(seq_name, current_value) values(?,?)";
    /**
     * 加载序列sql
     */
    private static volatile String LOAD_SQL = "select * from %s where seq_name=?";
    /**
     * 锁定序列sql
     */
    private static volatile String LOCK_SQL = "update %s set current_value=? where seq_name=? and current_value=?";
    /**
     * 数据源
     */
    private DataSource dataSource;
    /**
     * 表名称，默认表名global_seq
     */
    private String tableName = "global_seq";

    public GlobalSeqRepositoryImpl(DataSource dataSource, String table) {
        this.dataSource = dataSource;
        if(tableName != null) {
            this.tableName = table;
        }
        // 替换表名
        GlobalSeqRepositoryImpl.CREATE_SQL = String.format(GlobalSeqRepositoryImpl.CREATE_SQL, tableName);
        log.info("创建序列sql:{}", CREATE_SQL);
        GlobalSeqRepositoryImpl.LOAD_SQL = String.format(GlobalSeqRepositoryImpl.LOAD_SQL, tableName);
        log.info("加载序列sql:{}", CREATE_SQL);
        GlobalSeqRepositoryImpl.LOCK_SQL = String.format(GlobalSeqRepositoryImpl.LOCK_SQL, tableName);
        log.info("锁定序列sql:{}", CREATE_SQL);
    }

    @Override
    public int createSeq(GlobalSeqPo po) {
        log.info("创建序列:{}", po);
        Connection connection = null;
        PreparedStatement pst = null;
        try {
            connection = dataSource.getConnection();
            pst = connection.prepareStatement(CREATE_SQL);
            pst.setString(1, po.getSeqName());
            pst.setLong(2, po.getCurrentValue());
            // 执行返回
            return pst.executeUpdate();
        } catch (SQLException e) {
            log.error("创建序列sql异常:{}", e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            try {
                if(pst != null) {
                    pst.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                log.error("创建序列，关闭sql资源异常:{}", e.getMessage(), e);
            }
        }
    }

    @Override
    public Optional<GlobalSeqPo> loadSeq(String seqName) {
        log.info("加载序列:{}", seqName);
        Connection connection = null;
        PreparedStatement pst = null;
        ResultSet rs = null;

        try {
            connection = dataSource.getConnection();
            pst = connection.prepareStatement(LOAD_SQL);
            pst.setString(1, seqName);

            rs = pst.executeQuery();

            // 未查到
            if (!rs.next()) {
                return Optional.empty();
            }

            // 组装返回
            String seq = rs.getString(SEQ_NAME);
            Long currentValue = rs.getLong(CURRENT_VALUE);
            return Optional.of(new GlobalSeqPo(seq, currentValue));
        } catch (SQLException e) {
            log.error("加载序列sql异常:{}", e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            try {
                if(rs != null) {
                    rs.close();
                }
                if(pst != null) {
                    pst.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                log.error("加载序列，关闭sql资源异常:{}", e.getMessage(), e);
            }
        }
    }

    @Override
    public Optional<GlobalSeqPo> lockSeq(String seqName, long step, long old) {
        log.info("锁定序列:{},{},{}", seqName, step, old);
        Connection connection = null;
        PreparedStatement pst = null;
        try {
            connection = dataSource.getConnection();
            pst = dataSource.getConnection().prepareStatement(LOCK_SQL);
            pst.setLong(1, old + step);
            pst.setString(2, seqName);
            pst.setLong(3, old);
            // 执行返回
            int result = pst.executeUpdate();

            if(result == 1) {
                return Optional.of(new GlobalSeqPo(seqName, old + step));
            }
            return Optional.empty();
        } catch (SQLException e) {
            log.error("锁定序列sql异常:{}", e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            try {
                if(pst != null) {
                    pst.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                log.error("锁定序列，关闭sql资源异常:{}", e.getMessage(), e);
            }
        }
    }

}
