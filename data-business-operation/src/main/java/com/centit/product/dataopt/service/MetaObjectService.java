package com.centit.product.dataopt.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.product.dataopt.core.BizModel;
import com.centit.support.database.utils.PageDesc;

import java.util.Map;

public interface MetaObjectService {

    Map<String, Object> getObjectById(String tableId, Map<String, Object> pk);

    @Deprecated
    BizModel getObjectAsBizModel(String tableId, Map<String, Object> pk, int withChildrenDeep);

    Map<String, Object> getObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep);

    int saveObject(String tableId, Map<String, Object> object);

    int updateObject(String tableId, Map<String, Object> object);

    int mergeObject(String tableId, Map<String, Object> object);

    int saveObject(String tableId, BizModel bizModel);

    int mergeObject(String tableId, BizModel bizModel);

    void deleteObjectBy(String tableId, Map<String, Object> pk);

    JSONArray listObjectsByProperties(String tableId, Map<String, Object> filter);

    JSONArray pageQueryObjects(String tableId, Map<String, Object> params, PageDesc pageDesc);
    JSONArray pageQueryObjects(String tableId, String paramDriverSql, Map<String, Object> params, PageDesc pageDesc);
}
