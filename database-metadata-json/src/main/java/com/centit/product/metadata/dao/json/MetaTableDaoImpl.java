package com.centit.product.metadata.dao.json;

import com.alibaba.fastjson2.JSONArray;
import com.centit.product.metadata.dao.MetaTableDao;
import com.centit.product.metadata.po.MetaTable;
import com.centit.support.database.utils.PageDesc;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class MetaTableDaoImpl implements MetaTableDao {

    @Override
    public MetaTable getObjectById(Object tableId) {
        return null;
    }

    @Override
    public MetaTable getObjectCascadeById(Object tableId) {
        return null;
    }

    @Override
    public MetaTable getObjectWithReferences(Object tableId) {
        return null;
    }

    @Override
    public void saveNewObject(MetaTable tableInfo) {

    }

    @Override
    public MetaTable getMetaTable(String databaseCode, String tableName) {
        return null;
    }

    @Override
    public JSONArray getMetaTableList(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public JSONArray getMetaTableListWithTableOptRelation(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public boolean isTableExist(String tableName, String dataBaseCode) {
        return false;
    }

    @Override
    public MetaTable fetchObjectReference(MetaTable object, String columnName) {
        return null;
    }

    @Override
    public MetaTable fetchObjectReferences(MetaTable object) {
        return null;
    }

    @Override
    public JSONArray listObjectsByPropertiesAsJson(Map<String, Object> filterMap, PageDesc pageDesc) {
        return null;
    }

    @Override
    public List<MetaTable> listObjectsByProperties(Map<String, Object> filterMap) {
        return null;
    }

    @Override
    public List<MetaTable> listObjectsByFilter(String sqlWhere, Object[] params) {
        return null;
    }

    @Override
    public int deleteObjectReferences(MetaTable object) {
        return 0;
    }

    @Override
    public int deleteObject(MetaTable object) {
        return 0;
    }

    @Override
    public int updateObject(MetaTable object) {
        return 0;
    }

    @Override
    public int mergeObject(MetaTable object) {
        return 0;
    }
}
