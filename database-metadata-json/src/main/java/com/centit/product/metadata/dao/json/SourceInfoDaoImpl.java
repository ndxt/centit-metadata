package com.centit.product.metadata.dao.json;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.support.database.utils.PageDesc;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository("sourceInfoDao")
public class SourceInfoDaoImpl implements SourceInfoDao{
    @Override
    public List<SourceInfo> listDatabase() {
        return null;
    }

    @Override
    public List<SourceInfo> listDatabaseByOsId(String osId) {
        return null;
    }

    @Override
    public SourceInfo getDatabaseInfoById(String databaseCode) {
        return null;
    }

    @Override
    public String getNextKey() {
        return null;
    }

    @Override
    public JSONArray queryDatabaseAsJson(String databaseName, PageDesc pageDesc) {
        return null;
    }

    @Override
    public int countDataBase(Map<String, Object> params) {
        return 0;
    }

    @Override
    public List<SourceInfo> listObjectsByProperties(Map<String, Object> filterMap) {
        return null;
    }

    @Override
    public JSONArray listObjectsByPropertiesAsJson(Map<String, Object> filterMap, PageDesc pageDesc) {
        return null;
    }

    @Override
    public void saveNewObject(SourceInfo sourceInfo) {

    }

    @Override
    public int mergeObject(SourceInfo sourceInfo) {
        return 0;
    }

    @Override
    public int deleteObjectById(Object id) {
        return 0;
    }

    @Override
    public List<SourceInfo> listObjects() {
        return null;
    }
}
