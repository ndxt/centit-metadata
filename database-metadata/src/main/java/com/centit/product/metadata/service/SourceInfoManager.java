package com.centit.product.metadata.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.jdbc.service.BaseEntityManager;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;

public interface SourceInfoManager extends BaseEntityManager<SourceInfo,String> {

    List<SourceInfo> listDatabase();

    void saveNewObject(SourceInfo sourceInfo);

    void mergeObject(SourceInfo sourceInfo);

    String getNextKey();

    Map<String, SourceInfo> listDatabaseToDBRepo();

    List<SourceInfo> listObjects(Map<String, Object> map);

    JSONArray listDatabaseAsJson(Map<String, Object> filterMap, PageDesc pageDesc);

    JSONArray queryDatabaseAsJson(String databaseName, PageDesc pageDesc);

    List<SourceInfo> listDatabaseByOsId(String osId);
}

