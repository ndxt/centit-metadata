package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.product.metadata.service.DatabaseRunTime;
import com.centit.product.metadata.transaction.AbstractSourceConnectThreadHolder;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.DatabaseAccess;
import com.centit.support.database.utils.PersistenceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author zhf
 */
@Service
public class DatabaseRunTimeImpl implements DatabaseRunTime {

    @Autowired
    private SourceInfoDao sourceInfoDao;

    private SourceInfo fetchDataSource(String databaseCode) {
        return sourceInfoDao.getDatabaseInfoById(databaseCode);
    }

    @Override
    public JSONArray query(String databaseId, String sql, Object[] params) {
        try {
            try (Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(fetchDataSource(databaseId))) {
                return DatabaseAccess.findObjectsAsJSON(conn, sql, params);
            }
        } catch (SQLException | IOException e) {
            throw new ObjectException(PersistenceException.DATABASE_OPERATE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public JSONArray query(String databaseId, String sql) {
        try {
            try (Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(fetchDataSource(databaseId))) {
                return DatabaseAccess.findObjectsAsJSON(conn, sql);
            }
        } catch (SQLException | IOException e){
            throw new ObjectException(PersistenceException.DATABASE_OPERATE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public int execute(String databaseId, String sql, Object[] params) {
        try {
            Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(fetchDataSource(databaseId));
            return DatabaseAccess.doExecuteSql(conn, sql, params);
        } catch (SQLException e){
            throw new ObjectException(PersistenceException.DATABASE_OPERATE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public int execute(String databaseId, String sql) {
        try {
             Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(fetchDataSource(databaseId));
             return DatabaseAccess.doExecuteSql(conn, sql) ? 1 : 0;
        } catch (SQLException e){
            throw new ObjectException(PersistenceException.DATABASE_OPERATE_EXCEPTION, e.getMessage());
        }
    }
}
