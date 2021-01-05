package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.centit.product.metadata.dao.DatabaseInfoDao;
import com.centit.product.metadata.po.DatabaseInfo;
import com.centit.product.metadata.service.DatabaseRunTime;
import com.centit.support.common.ObjectException;
import com.centit.support.database.transaction.ConnectThreadHolder;
import com.centit.support.database.utils.DataSourceDescription;
import com.centit.support.database.utils.DatabaseAccess;
import com.centit.support.database.utils.PersistenceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

@Service
public class DatabaseRunTimeImpl implements DatabaseRunTime {

    @Autowired
    private DatabaseInfoDao databaseInfoDao;

    private DataSourceDescription fetchDataSource(String databaseCode) {
        DatabaseInfo databaseInfo = databaseInfoDao.getDatabaseInfoById(databaseCode);
        return DataSourceDescription.valueOf(databaseInfo);
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
