package com.centit.product.metadata.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.support.database.utils.PageDesc;

import java.util.Collection;
import java.util.Map;

public interface MetaObjectService {

    String getTableId(String databaseCode, String tableName);

    Map<String, Object> getObjectById(String tableId, Map<String, Object> pk);

    Map<String, Object> getObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep);

    int saveObject(String tableId, Map<String, Object> object);

    int updateObject(String tableId, Map<String, Object> object);

    int updateObjectByProperties(String tableId, final Collection<String> fields,final Map<String, Object> object);

    int updateObjectsByProperties(String tableId, final Collection<String> fields,
                                  final Map<String, Object> fieldValues,final Map<String, Object> properties);

    void deleteObject(String tableId, Map<String, Object> pk);

    int saveObjectWithChildren(String tableId, Map<String, Object> object);

    int updateObjectWithChildren(String tableId, Map<String, Object> object);

    void deleteObjectWithChildren(String tableId, Map<String, Object> pk);

    JSONArray listObjectsByProperties(String tableId, Map<String, Object> filter);

    JSONArray pageQueryObjects(String tableId, Map<String, Object> params, PageDesc pageDesc);

    JSONArray pageQueryObjects(String tableId, Map<String, Object> params, String [] fields,PageDesc pageDesc);

    JSONArray pageQueryObjects(String tableId, String paramDriverSql, Map<String, Object> params, PageDesc pageDesc);
}
