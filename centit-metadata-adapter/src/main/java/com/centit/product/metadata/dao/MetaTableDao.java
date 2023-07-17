package com.centit.product.metadata.dao;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.po.MetaTable;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;

public interface MetaTableDao {

    MetaTable getObjectById(Object tableId);

    MetaTable getObjectCascadeById(Object tableId);

    MetaTable getObjectWithReferences(Object tableId);

    void saveNewObject(MetaTable tableInfo);

    MetaTable getMetaTable(String databaseCode, String tableName);

    /**
     * 根据osId过滤MetaTable数据
     * @param parameters parameters
     * @return JSONArray
     */
    JSONArray getMetaTableList(Map<String, Object> parameters) ;

    /**
     * 根据optId过滤MetaTable数据
     * @param parameters parameters
     * @return JSONArray
     */
    JSONArray getMetaTableListWithTableOptRelation(Map<String, Object> parameters);

    boolean isTableExist(String tableName, String dataBaseCode);


    MetaTable fetchObjectReference(MetaTable object, String columnName);

    MetaTable fetchObjectReferences(MetaTable object);
    JSONArray listObjectsByPropertiesAsJson(Map<String, Object> filterMap, PageDesc pageDesc);

    List<MetaTable> listObjectsByProperties(Map<String, Object> filterMap);

    List<MetaTable> listObjectsByFilter(String sqlWhere, Object[] params);

    int deleteObjectReferences(MetaTable object);

    int deleteObject(MetaTable object);

    int updateObject(MetaTable object);

    int mergeObject(MetaTable object);

}
