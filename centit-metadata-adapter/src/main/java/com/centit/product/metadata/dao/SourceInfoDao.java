package com.centit.product.metadata.dao;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;


public interface SourceInfoDao {

    List<SourceInfo> listDatabase();

    List<SourceInfo> listDatabaseByOsId(String osId);

    SourceInfo getDatabaseInfoById(String databaseCode);

    //jdbc
    String getNextKey();

    JSONArray queryDatabaseAsJson(String databaseName, PageDesc pageDesc);

    /**
     * 统计租户下数据个数
     * @param params 过滤参数
     * @return 统计租户下数据个数
     */
    int countDataBase(Map<String,Object> params);

    List<SourceInfo> listObjectsByProperties(Map<String, Object> filterMap);

    JSONArray listObjectsByPropertiesAsJson(Map<String, Object> filterMap, PageDesc pageDesc);

    void saveNewObject(SourceInfo sourceInfo);

    int mergeObject(SourceInfo sourceInfo);

    int deleteObjectById(Object id);

    List<SourceInfo> listObjects();

}
