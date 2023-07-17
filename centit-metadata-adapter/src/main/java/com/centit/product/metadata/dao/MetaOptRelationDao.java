package com.centit.product.metadata.dao;


import com.centit.product.metadata.po.MetaOptRelation;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;

public interface MetaOptRelationDao {

    void saveNewObject(MetaOptRelation relation);

    MetaOptRelation getObjectById(Object id);

    MetaOptRelation getObjectByProperties(Map<String, Object> filterMap);

    int updateObject(MetaOptRelation relation);

    int deleteObjectById(Object id);
    List<MetaOptRelation> listObjectsByProperties(Map<String, Object> filterMap);

    List<MetaOptRelation> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc);
}
