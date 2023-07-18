package com.centit.product.metadata.dao.json;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.framework.components.CodeRepositoryCache;
import com.centit.product.metadata.dao.MetaTableDao;
import com.centit.product.metadata.po.MetaTable;
import com.centit.support.common.CachedMap;
import com.centit.support.database.utils.PageDesc;
import org.apache.commons.lang3.tuple.MutablePair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Repository("metaTableDao")
public class MetaTableDaoImpl implements MetaTableDao {

    @Value("${app.home:./}")
    private String appHome;

    private CachedMap<String, MetaTable> metaTableCache =
        new CachedMap<>(
            ( tableId )->  this.loadMetaTable(tableId),
            CodeRepositoryCache.CACHE_EXPIRE_EVERY_DAY );

    private MetaTable loadMetaTable(String tableId){
        String tableFile = appHome + File.separator + "config" +
             File.separator +  "metadata" + File.separator + tableId +".json";
        try {
            JSONObject tableJson = JSON.parseObject(new FileInputStream(tableFile));
            MetaTable metaTable = tableJson.toJavaObject(MetaTable.class);
            //tableJson.getJSONArray()

            return metaTable;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public MetaTable getObjectById(Object tableId) {
        return metaTableCache.getCachedValue((String) tableId);
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
        return object;
    }

    @Override
    public MetaTable fetchObjectReferences(MetaTable object) {
        return object;
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
