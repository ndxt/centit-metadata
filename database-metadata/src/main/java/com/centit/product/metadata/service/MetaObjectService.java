package com.centit.product.metadata.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.support.database.utils.PageDesc;

import java.util.Collection;
import java.util.Map;

public interface MetaObjectService {

    Map<String, Object> getObjectById(String tableId, Map<String, Object> pk);

    Map<String, Object> getObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep);

    Map<String, Object> makeNewObject(String tableId, Map<String, Object> extParams);
    Map<String, Object> makeNewObject(String tableId);

    int saveObject(String tableId, Map<String, Object> object);
    int saveObject(String tableId, Map<String, Object> object, Map<String, Object> extParams);

    int updateObject(String tableId, Map<String, Object> object);

    int updateObjectFields(String tableId, final Collection<String> fields,final Map<String, Object> object);

    int updateObjectsByProperties(String tableId, final Collection<String> fields,
                                  final Map<String, Object> fieldValues,final Map<String, Object> filterProperties);

    int updateObjectsByProperties(String tableId,
                                  final Map<String, Object> fieldValues,final Map<String, Object> filterProperties);

    void deleteObject(String tableId, Map<String, Object> pk);

    int saveObjectWithChildren(String tableId, Map<String, Object> object);
    int saveObjectWithChildren(String tableId, Map<String, Object> object, Map<String, Object> extParams);

    int updateObjectWithChildren(String tableId, Map<String, Object> object);

    void deleteObjectWithChildren(String tableId, Map<String, Object> pk);

    JSONArray listObjectsByProperties(String tableId, Map<String, Object> filter);

    JSONArray pageQueryObjects(String tableId, Map<String, Object> params, PageDesc pageDesc);

    JSONArray pageQueryObjects(String tableId, String extFilter, Map<String, Object> params, String [] fields,PageDesc pageDesc);

    JSONArray pageQueryObjects(String tableId, Map<String, Object> params, String [] fields,PageDesc pageDesc);

    JSONArray pageQueryObjects(String tableId, String namedSql, Map<String, Object> params, PageDesc pageDesc);

    JSONArray paramDriverPageQueryObjects(String tableId, String paramDriverSql, Map<String, Object> params, PageDesc pageDesc);

}
