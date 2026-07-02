package com.centit.product.metadata.transaction;

import com.centit.product.metadata.api.ISourceInfo;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.database.utils.DBType;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author zhf
 */
public abstract class AbstractDBConnectPools {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDBConnectPools.class);
    private static final
    ConcurrentHashMap<ISourceInfo, HikariDataSource> DATABASE_SOURCE_POOLS
        = new ConcurrentHashMap<>();

    private AbstractDBConnectPools() {
        throw new IllegalAccessError("Utility class");
    }

    private static HikariDataSource createDataSource(ISourceInfo dsDesc) {
        HikariDataSource ds = new HikariDataSource();
        //ds.setConnectionErrorRetryAttempts(3);
        DBType dbType=DBType.mapDBType(dsDesc.getDatabaseUrl());

        ds.setDriverClassName(DBType.getDbDriver(dbType));
        ds.setUsername(dsDesc.getUsername());
        ds.setPassword(dsDesc.getClearPassword());

        ds.setJdbcUrl(dsDesc.getDatabaseUrl());

        ds.setMaximumPoolSize(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("maxActive"), 50));
        ds.setMaxLifetime(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("maxLifetime"), 180000));
        ds.setIdleTimeout(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("idleTimeout"), 6000));

        ds.setConnectionTimeout(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("connectionTimeout"), 5000));

        ds.setMinimumIdle(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("minIdle"), 5));

        String validationQuery = StringBaseOpt.castObjectToString(dsDesc.getExtProp("validationQuery"));
        if(StringUtils.isBlank(validationQuery)){
            validationQuery = DBType.getDBValidationQuery(dbType);
        }
        ds.setValidationTimeout(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("validationTimeout"), 5000));

        boolean testWhileIdle = BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("testWhileIdle"), true);

        if(testWhileIdle && StringUtils.isNotBlank(validationQuery)){
            ds.setConnectionTestQuery(validationQuery);
        }
        return ds;
    }

    public static void refreshDataSource(ISourceInfo dsDesc) {
        if(DATABASE_SOURCE_POOLS.containsKey(dsDesc)){
            HikariDataSource ds = createDataSource(dsDesc);
            HikariDataSource oldDs = DATABASE_SOURCE_POOLS.put(dsDesc, ds);
            if(oldDs!=null) {
                oldDs.close();
            }
        }
    }
    public static void delDataSource(ISourceInfo dsDesc) {
        if(DATABASE_SOURCE_POOLS.containsKey(dsDesc)){
            HikariDataSource oldDs = DATABASE_SOURCE_POOLS.get(dsDesc);
            if(oldDs!=null) {
                oldDs.close();
            }
            DATABASE_SOURCE_POOLS.remove(dsDesc);
        }
    }

    public static Connection getDbcpConnect(ISourceInfo dsDesc) throws SQLException {
        // 使用 computeIfAbsent 保证池的懒创建线程安全，无需 synchronized 全局锁。
        // 之前的 synchronized 会让所有数据源、所有线程串行获取连接，一旦某个线程因池耗尽而
        // 阻塞（最长 connectionTimeout），其余线程全部被锁死，引发雪崩式超时。
        HikariDataSource ds = DATABASE_SOURCE_POOLS.computeIfAbsent(dsDesc, AbstractDBConnectPools::createDataSource);
        try {
            Connection conn = ds.getConnection();
            conn.setAutoCommit(false);
            return conn;
        } catch (SQLException e) {
            // 池耗尽时快速失败并打印池状态，便于定位是连接泄漏还是慢查询。
            // 不再 sleep+重试：池耗尽期间重试只会再次阻塞 connectionTimeout，
            // 且原先重试发生在 synchronized 方法内，归还连接的线程也进不来，雪崩被锁死。
            logPoolStatus(dsDesc, ds, e);
            throw e;
        }
    }

    /**
     * 打印 HikariCP 池状态，用于连接获取失败时的诊断。
     */
    private static void logPoolStatus(ISourceInfo dsDesc, HikariDataSource ds, SQLException e) {
        try {
            if (ds.isClosed()) {
                logger.error("获取数据库连接失败，数据源 [{}] 的连接池已关闭: {}",
                    dsDesc.getDatabaseCode(), e.getMessage(), e);
                return;
            }
            // getHikariPoolMXBean 在池启动后才可用
            HikariPoolMXBean mxBean = ds.getHikariPoolMXBean();
            if (mxBean != null) {
                logger.error("获取数据库连接失败，数据源 [{}] 池状态: active={}, idle={}, 等待线程数={}, 总连接={}, max={}, 错误: {}",
                    dsDesc.getDatabaseCode(),
                    mxBean.getActiveConnections(), mxBean.getIdleConnections(),
                    mxBean.getThreadsAwaitingConnection(), mxBean.getTotalConnections(),
                    ds.getMaximumPoolSize(), e.getMessage(), e);
            } else {
                logger.error("获取数据库连接失败，数据源 [{}]: {}", dsDesc.getDatabaseCode(), e.getMessage(), e);
            }
        } catch (Exception ignore) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void closeConnect(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public static void testConnect(SourceInfo sourceInfo) throws SQLException {
        // 测试能否成功获取连接：获取成功即视为连通性正常，由 try-with-resources 自动关闭
        try (HikariDataSource ds = createDataSource(sourceInfo);
             Connection conn = ds.getConnection()) {
        }
    }

}
