package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.jdbc.service.BaseEntityManagerImpl;
import com.centit.product.adapter.po.SourceInfo;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.service.SourceInfoManager;
import com.centit.support.database.utils.PageDesc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("databaseInfoManager")
@Transactional
public class SourceInfoManagerImpl extends BaseEntityManagerImpl<SourceInfo, String, SourceInfoDao>
        implements SourceInfoManager {

    //private static final SysOptLog sysOptLog = SysOptLogFactoryImpl.getSysOptLog();

    @Override
    @Autowired
    public void setBaseDao(SourceInfoDao baseDao) {
        super.baseDao = baseDao;
    }


    @Override
    public List<SourceInfo> listDatabase() {
        List<SourceInfo> database = baseDao.listDatabase();
        return database;
    }

    @Override
    public void saveNewObject(SourceInfo sourceInfo) {
        baseDao.saveNewObject(sourceInfo);
    }

    @Override
    public String getNextKey() {
        return baseDao.getNextKey();
    }

    @Override
    public void mergeObject(SourceInfo sourceInfo){

        baseDao.mergeObject(sourceInfo);
    }

    @Override
    @Transactional(readOnly = true)
    public Map<String, SourceInfo> listDatabaseToDBRepo() {
        List<SourceInfo> dbList=baseDao.listObjects();
        Map<String, SourceInfo> dbmap = new HashMap<>();
        if(dbList != null){
            for(SourceInfo db : dbList){
                dbmap.put(db.getDatabaseCode(),db);
            }
        }
        return dbmap;
    }

    @Override
    public List<SourceInfo> listObjects(Map<String, Object> map){
        return baseDao.listObjects(map);
    }

    @Override
    public JSONArray listDatabaseAsJson(Map<String, Object> filterMap, PageDesc pageDesc){
        return baseDao.listObjectsAsJson(filterMap, pageDesc);
    }

    @Override
    public JSONArray queryDatabaseAsJson(String databaseName, PageDesc pageDesc){
        return baseDao.queryDatabaseAsJson(databaseName, pageDesc);
    }

    @Override
    @Transactional
    public List<SourceInfo> listDatabaseByOsId(String osId) {
        return baseDao.listObjectsByProperty("osId",osId);
    }
}

