package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.product.metadata.service.DatabaseRunTime;
import com.centit.support.common.ObjectException;
import com.centit.support.database.transaction.ConnectThreadHolder;
import com.centit.support.database.utils.DataSourceDescription;
import com.centit.support.database.utils.DatabaseAccess;
import com.centit.support.database.utils.JdbcConnect;
import com.centit.support.database.utils.PersistenceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

@Service
public class DatabaseRunTimeImpl implements DatabaseRunTime {

    @Autowired
    private IntegrationEnvironment integrationEnvironment;

    private DataSourceDescription fetchDataSource(String databaseCode) throws SQLException {
        DatabaseInfo databaseInfo = integrationEnvironment.getDatabaseInfo(databaseCode);
        return JdbcConnect.mapDataSource(databaseInfo);
    }
    @Override
    public JSONArray query(String databaseId, String sql, Object[] params) {
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(fetchDataSource(databaseId));
            return DatabaseAccess.findObjectsAsJSON(conn, sql, params);
        } catch (SQLException | IOException e){
            throw new ObjectException(PersistenceException.DATABASE_OPERATE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public JSONArray query(String databaseId, String sql) {
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(fetchDataSource(databaseId));
            return DatabaseAccess.findObjectsAsJSON(conn, sql);
        } catch (SQLException | IOException e){
            throw new ObjectException(PersistenceException.DATABASE_OPERATE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public int execute(String databaseId, String sql, Object[] params) {
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(fetchDataSource(databaseId));
            return DatabaseAccess.doExecuteSql(conn, sql, params);
        } catch (SQLException e){
            throw new ObjectException(PersistenceException.DATABASE_OPERATE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public int execute(String databaseId, String sql) {
        try {
            Connection conn = ConnectThreadHolder.fetchConnect(fetchDataSource(databaseId));
            return DatabaseAccess.doExecuteSql(conn, sql)?1:0;
        } catch (SQLException e){
            throw new ObjectException(PersistenceException.DATABASE_OPERATE_EXCEPTION, e.getMessage());
        }
    }
}
