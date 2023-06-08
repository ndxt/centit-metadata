package com.centit.product.metadata.transaction;

import com.alibaba.druid.pool.DruidDataSource;
import com.centit.product.adapter.api.ISourceInfo;
import com.centit.product.adapter.po.SourceInfo;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.database.utils.DBType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author zhf
 */
public abstract class AbstractDruidConnectPools {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDruidConnectPools.class);
    private static final
    ConcurrentHashMap<ISourceInfo, DruidDataSource> DRUID_DATA_SOURCE_POOLS
        = new ConcurrentHashMap<>();

    private AbstractDruidConnectPools() {
        throw new IllegalAccessError("Utility class");
    }

    private static DruidDataSource createDataSource(ISourceInfo dsDesc) {
        DruidDataSource ds = new DruidDataSource();
        //失败时是否进行重试连接    true:不进行重试   false：进行重试    设置为false时达蒙数据库会出现问题（会导致达蒙连接撑爆挂掉）
        ds.setBreakAfterAcquireFailure(BooleanBaseOpt.castObjectToBoolean(dsDesc.getExtProp("breakAfterAcquireFailure"),false));
        ds.setTimeBetweenConnectErrorMillis(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("timeBetweenConnectErrorMillis"), 6000));
        ds.setConnectionErrorRetryAttempts(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("connectionErrorRetryAttempts"), 1));
        //ds.setConnectionErrorRetryAttempts(3);
        DBType dbType=DBType.mapDBType(dsDesc.getDatabaseUrl());
        if(dbType.equals(DBType.Oracle)){
            ds.setConnectionProperties("remarksReporting=true");
        }
        ds.setDriverClassName(DBType.getDbDriver(dbType));
        ds.setUsername(dsDesc.getUsername());
        ds.setPassword(dsDesc.getClearPassword());
        ds.setUrl(dsDesc.getDatabaseUrl());
        ds.setInitialSize(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("initialSize"), 5));
        ds.setMaxActive(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("maxActive"), 25));
        ds.setMaxWait(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("maxWaitMillis"), 10000));
        ds.setMinIdle(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("minIdle"), 5));

        String validationQuery = StringBaseOpt.castObjectToString(dsDesc.getExtProp("validationQuery"));
        if(StringUtils.isBlank(validationQuery)){
            validationQuery = DBType.getDBValidationQuery(dbType);
        }

        boolean testWhileIdle = BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("testWhileIdle"), true);

        if(testWhileIdle && StringUtils.isNotBlank(validationQuery)){
            ds.setValidationQuery(validationQuery);
            ds.setTestWhileIdle(true);
        }

        ds.setValidationQueryTimeout(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("validationQueryTimeout"), 1000 * 10));
        ds.setKeepAlive(BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("keepAlive"), true));
        ds.setTimeBetweenEvictionRunsMillis(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("timeBetweenEvictionRunsMillis"), 60000));
        ds.setMinEvictableIdleTimeMillis(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("minEvictableIdleTimeMillis"), 300000));
        boolean removeAbandoned = BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("removeAbandoned"), true);
        ds.setRemoveAbandoned(removeAbandoned);
        if(removeAbandoned) {
            ds.setRemoveAbandonedTimeout(NumberBaseOpt.castObjectToInteger(
                dsDesc.getExtProp("removeAbandonedTimeout"), 900));
        }
        ds.setLogAbandoned(BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("logAbandoned"), true));
        ds.setTestOnBorrow(BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("testOnBorrow"), true));
        ds.setTestOnReturn(BooleanBaseOpt.castObjectToBoolean(
            dsDesc.getExtProp("testOnReturn"), false));
        ds.setMaxWait(NumberBaseOpt.castObjectToInteger(
            dsDesc.getExtProp("maxWait"), 60000));
        return ds;
    }

    public static void refreshDataSource(ISourceInfo dsDesc) {
        if(DRUID_DATA_SOURCE_POOLS.containsKey(dsDesc)){ // 已经有连接池的情况下更换连接池
            DruidDataSource ds = createDataSource(dsDesc);
            DruidDataSource oldDs = DRUID_DATA_SOURCE_POOLS.put(dsDesc, ds);
            if(oldDs!=null) {
                oldDs.close();
            }
        }
    }

    public static synchronized Connection getDbcpConnect(ISourceInfo dsDesc) throws SQLException {
        DruidDataSource ds = DRUID_DATA_SOURCE_POOLS.get(dsDesc);
        if (ds == null) {
            ds = createDataSource(dsDesc);
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
        DruidDataSource ds = createDataSource(sourceInfo);
        Connection conn=null;
        try  {
            conn = ds.getConnection();
            conn.close();
            ds.close();
        }
        finally {
            if(conn!=null) {
                conn.close();
            }
            ds.close();
        }
    }

}
