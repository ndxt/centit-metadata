package com.centit.product.metadata.transaction;

import com.centit.product.metadata.api.ISourceInfo;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.database.utils.DBType;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
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

    public static synchronized Connection getDbcpConnect(ISourceInfo dsDesc) throws SQLException, InterruptedException {
        HikariDataSource ds = DATABASE_SOURCE_POOLS.get(dsDesc);
        if (ds == null) {
            ds = createDataSource(dsDesc);
            DATABASE_SOURCE_POOLS.put(dsDesc, ds);
        }
        try {
            Connection conn = ds.getConnection();
            conn.setAutoCommit(false);
            return conn;
        }catch (SQLException e) {
            throw e;
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
        HikariDataSource ds = createDataSource(sourceInfo);
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
