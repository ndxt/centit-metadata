package com.centit.support.metadata.utils;

import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.support.database.utils.DataSourceDescription;
import com.centit.support.database.utils.DbcpConnectPools;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcConnect {

    public static Connection getConn(DatabaseInfo dbinfo) throws SQLException {
        DataSourceDescription desc=new DataSourceDescription();
        desc.setConnUrl(dbinfo.getDatabaseUrl());
        desc.setUsername(dbinfo.getUsername());
        desc.setPassword(dbinfo.getClearPassword());
        desc.setMaxIdle(10);
        desc.setMaxTotal(20);
        desc.setMaxWaitMillis(20000);
        return DbcpConnectPools.getDbcpConnect(desc);//.getConn();
    }
}
