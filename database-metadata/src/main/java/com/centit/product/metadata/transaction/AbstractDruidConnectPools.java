package com.centit.product.metadata.transaction;

import com.alibaba.druid.pool.DruidDataSource;
import com.centit.product.adapter.api.ISourceInfo;
import com.centit.product.adapter.po.SourceInfo;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.database.utils.DBType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author zhf
 */
public abstract class AbstractDruidConnectPools {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDruidConnectPools.class);
    private static final
    Map<ISourceInfo, DruidDataSource> DRUID_DATA_SOURCE_POOLS
        = new ConcurrentHashMap<>();

    private AbstractDruidConnectPools() {
        throw new IllegalAccessError("Utility class");
    }

    private static DruidDataSource mapDataSource(ISourceInfo dsDesc) {
        DruidDataSource ds = new DruidDataSource();
        ds.setBreakAfterAcquireFailure(false);
        ds.setDriverClassName(DBType.getDbDriver(DBType.mapDBType(dsDesc.getDatabaseUrl())));
        ds.setUsername(dsDesc.getUsername());
        ds.setPassword(dsDesc.getClearPassword());
        ds.setUrl(dsDesc.getDatabaseUrl());
        ds.setInitialSize(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("initialSize"), 5));
        ds.setMaxActive(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("maxActive"), 10));
        ds.setMaxWait(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("maxWaitMillis"), 10000));
        ds.setMinIdle(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("minIdle"), 5));
        ds.setValidationQuery(StringBaseOpt.castObjectToString(dsDesc.getExtProp("validationQuery"),
            "select 1"));
        ds.setTestWhileIdle(BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("testWhileIdle"), true));
        ds.setValidationQueryTimeout(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("validationQueryTimeout"), 1000 * 10));
        ds.setKeepAlive(BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("keepAlive"), true));
        ds.setTimeBetweenEvictionRunsMillis(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("timeBetweenEvictionRunsMillis"), 60000));
        ds.setMinEvictableIdleTimeMillis(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("minEvictableIdleTimeMillis"), 300000));
        ds.setRemoveAbandoned(BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("removeAbandoned"), true));
        ds.setRemoveAbandonedTimeout(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("removeAbandonedTimeout"), 80));
        ds.setLogAbandoned(BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("logAbandoned"), true));
        ds.setTestOnBorrow(BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("testOnBorrow"), true));
        ds.setTestOnReturn(BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("testOnReturn"), false));
        ds.setMaxWait(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("maxWait"), 60000));
        //创建连接时连接失败后 禁止重试连接
        ds.setBreakAfterAcquireFailure(BooleanBaseOpt.castObjectToBoolean(dsDesc.getExtProp("breakAfterAcquireFailure"),true));
        ds.setTimeBetweenConnectErrorMillis(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("timeBetweenConnectErrorMillis"), 6000));
        ds.setConnectionErrorRetryAttempts(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("connectionErrorRetryAttempts"), 1));
        return ds;
    }

    public static synchronized Connection getDbcpConnect(ISourceInfo dsDesc) throws SQLException {
        DruidDataSource ds = DRUID_DATA_SOURCE_POOLS.get(dsDesc);
        if (ds == null) {
            ds = mapDataSource(dsDesc);
            DRUID_DATA_SOURCE_POOLS.put(dsDesc, ds);
        }
        Connection conn = ds.getConnection();
        conn.setAutoCommit(false);
        return conn;
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
        DruidDataSource ds =mapDataSource(sourceInfo);
        try (Connection conn = ds.getConnection()) {
            conn.close();
            ds.close();
        }
    }

}
