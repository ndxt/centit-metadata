package com.centit.product.metadata.utils;

import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.support.database.utils.DataSourceDescription;
import com.centit.support.database.utils.DbcpConnectPools;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcConnect {

    public static DataSourceDescription mapDataSource(DatabaseInfo dbinfo){
        DataSourceDescription desc=new DataSourceDescription();
        desc.setConnUrl(dbinfo.getDatabaseUrl());
        desc.setUsername(dbinfo.getUsername());
        desc.setPassword(dbinfo.getClearPassword());
        desc.setMaxIdle(10);
        desc.setMaxTotal(20);
        desc.setMaxWaitMillis(20000);
        return desc;
    }

    public static Connection getConn(DatabaseInfo dbinfo) throws SQLException {
        return DbcpConnectPools.getDbcpConnect(mapDataSource(dbinfo));//.getConn();
    }
}
