package com.centit.product.metadata.dao;

import com.centit.product.metadata.po.MetaRelation;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;

public interface MetaRelationDao {

    MetaRelation getObjectById(Object id);

    void saveNewObject(MetaRelation object);

    int deleteObject(MetaRelation object);

    int updateObject(MetaRelation object);

    int saveObjectReference(MetaRelation object, String columnName);

    int saveObjectReferences(MetaRelation object);

    int deleteObjectReference(MetaRelation object, String columnName);

    MetaRelation getObjectByProperties(Map<String, Object> filterMap);

    List<MetaRelation> listRelationByTables(String parentTableId, String childTableId);

    List<MetaRelation> listObjectsByProperties(Map<String, Object> filterMap);

    List<MetaRelation> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc);

    MetaRelation fetchObjectReference(MetaRelation object, String columnName);

    MetaRelation fetchObjectReferences(MetaRelation object);

}
