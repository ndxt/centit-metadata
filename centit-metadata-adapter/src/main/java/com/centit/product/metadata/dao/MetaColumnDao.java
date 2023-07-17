package com.centit.product.metadata.dao;

import com.centit.product.metadata.po.MetaColumn;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;

public interface MetaColumnDao {

    MetaColumn getObjectById(MetaColumn id);

    void saveNewObject(MetaColumn columnInfo);

    int mergeObject(MetaColumn columnInfo);

    int deleteObject(MetaColumn object);

    int updateObject(MetaColumn object);

    List<MetaColumn> listObjectsByProperties(Map<String, Object> filterMap);

    List<MetaColumn> listObjectsByProperties(Map<String, Object> filterMap, PageDesc pageDesc);

    List<MetaColumn> listObjectsBySql(String querySql, Object[] params);
}
