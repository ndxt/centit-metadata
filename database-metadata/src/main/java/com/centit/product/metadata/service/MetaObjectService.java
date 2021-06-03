package com.centit.product.metadata.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.product.metadata.po.MetaTable;
import com.centit.support.database.utils.PageDesc;

import java.util.Collection;
import java.util.Map;

public interface MetaObjectService {

    Map<String, Object> getObjectById(String tableId, Map<String, Object> pk);
    Map<String, Object> getObjectWithChildren(String tableId, Map<String, Object> pk, int withChildrenDeep);

    /*void fetchObjectParents(Connection conn, Map<String, Object> mainObj,
                            MetaTable tableInfo) throws SQLException, IOException;

    void fetchObjectParent(Connection conn, Map<String, Object> mainObj,
                            MetaTable tableInfo, String parentName) throws SQLException, IOException;

    void fetchObjectRefrence(Connection conn, Map<String, Object> mainObj,
                              MetaTable tableInfo, String childName)  throws SQLException, IOException;

    void fetchObjectRefrences(Connection conn, Map<String, Object> mainObj,
                              MetaTable tableInfo, int withChildrenDeep)  throws SQLException, IOException;*/

    Map<String, Object> getObjectWithChildren(String tableId, Map<String, Object> pk, String [] fields,
                                              String [] parents, String [] children);

    Map<String, Object> fetchObjectParentAndChildren(MetaTable tableInfo, Map<String, Object> mainObj,
                                                            String [] parents, String [] children);

    Map<String, Object> makeNewObject(String tableId, Map<String, Object> extParams);
    Map<String, Object> makeNewObject(String tableId);

    int saveObject(String tableId, Map<String, Object> object);
    int saveObject(String tableId, Map<String, Object> object, Map<String, Object> extParams);

    int updateObject(String tableId, Map<String, Object> object);


    int updateObjectFields(String tableId, final Collection<String> fields,final Map<String, Object> object);

    int updateObjectsByProperties(String tableId,
                                  final Collection<String> fields,
                                  final Map<String, Object> fieldValues,
                                  final Map<String, Object> filterProperties);

    int updateObjectsByProperties(String tableId,
                                  final Map<String, Object> fieldValues,
                                  final Map<String, Object> filterProperties);

    void deleteObject(String tableId, Map<String, Object> pk);

    int saveObjectWithChildren(String tableId, Map<String, Object> object,int withChildrenDeep);
    int saveObjectWithChildren(String tableId, Map<String, Object> object, Map<String, Object> extParams,int withChildrenDeep);

    int updateObjectWithChildren(String tableId, Map<String, Object> object,int withChildrenDeep);

    void deleteObjectWithChildren(String tableId, Map<String, Object> pk,int withChildrenDeep);
    int mergeObjectWithChildren(String tableId, Map<String, Object> object,int withChildrenDeep);
    JSONArray listObjectsByProperties(String tableId, Map<String, Object> filter);

    JSONArray pageQueryObjects(String tableId, Map<String, Object> params, PageDesc pageDesc);

    JSONArray pageQueryObjects(String tableId, String extFilter, Map<String, Object> params, String [] fields,PageDesc pageDesc);

    JSONArray pageQueryObjects(String tableId, Map<String, Object> params, String [] fields,PageDesc pageDesc);

    JSONArray pageQueryObjects(String tableId, String namedSql, Map<String, Object> params, PageDesc pageDesc);

    JSONArray paramDriverPageQueryObjects(String tableId, String paramDriverSql, Map<String, Object> params, PageDesc pageDesc);

}
