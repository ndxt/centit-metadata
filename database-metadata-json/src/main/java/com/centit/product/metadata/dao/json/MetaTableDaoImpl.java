package com.centit.product.metadata.dao.json;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.framework.components.CodeRepositoryCache;
import com.centit.product.metadata.dao.MetaTableDao;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaRelDetail;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.product.metadata.po.MetaTable;
import com.centit.support.common.CachedMap;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
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
            JSONArray columns = tableJson.getJSONArray("columns");
            if(columns!=null){
                List<MetaColumn> cols = columns.toJavaList(MetaColumn.class);
                metaTable.setMdColumns(cols);
            }

            JSONArray relations = tableJson.getJSONArray("relations");
            if(relations!=null){
                List<MetaRelation> rels = new ArrayList<>(relations.size());
                for(Object obj : relations){
                    if(obj instanceof JSONObject){
                        JSONObject relation = (JSONObject) obj;
                        MetaRelation rel = relation.toJavaObject(MetaRelation.class);
                        JSONArray details = relation.getJSONArray("details");
                        if(details!=null){
                            rel.setRelationDetails(details.toJavaList(MetaRelDetail.class));
                        }
                        rels.add(rel);
                    }
                }
                metaTable.setMdRelations(rels);
            }
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
        return metaTableCache.getCachedValue((String) tableId);
    }

    @Override
    public MetaTable getObjectWithReferences(Object tableId) {
        return metaTableCache.getCachedValue((String) tableId);
    }

    @Override
    public void saveNewObject(MetaTable tableInfo) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public MetaTable getMetaTable(String databaseCode, String tableName) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public JSONArray getMetaTableList(Map<String, Object> parameters) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public JSONArray getMetaTableListWithTableOptRelation(Map<String, Object> parameters) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public boolean isTableExist(String tableName, String dataBaseCode) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
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
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public List<MetaTable> listObjectsByProperties(Map<String, Object> filterMap) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public List<MetaTable> listObjectsByFilter(String sqlWhere, Object[] params) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public List<MetaTable> listObjectsByFilter(String sqlWhere, Map<String, Object> filterMap){
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public int deleteObjectReferences(MetaTable object) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public int deleteObject(MetaTable object) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public int updateObject(MetaTable object) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }

    @Override
    public int mergeObject(MetaTable object) {
        throw new ObjectException(ObjectException.FUNCTION_NOT_SUPPORT,
            "该方法在当前版本下没有实现，请联系研发人员!");
    }
}
