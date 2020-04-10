package com.centit.product.metadata.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.vo.MetaTableCascade;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.utils.PageDesc;

import java.util.List;
import java.util.Map;

public interface MetaDataService {

    List<MetaTable> listAllMetaTables(String databaseCode);

    List<MetaTable> listAllMetaTablesWithDetail(String databaseCode);

    JSONArray listMetaTables(Map<String, Object> filterMap, PageDesc pageDes);

    List<DatabaseInfo> listDatabase(String osId);

    List<SimpleTableInfo> listRealTables(String databaseCode);

    void syncDb(String databaseCode, String recorder);

    void updateMetaTable(MetaTable metaTable);

    MetaTable getMetaTable(String tableId);

    MetaTable getMetaTableWithRelations(String tableId);

    MetaTable getMetaTable(String databaseCode, String tableName);

    MetaTable getMetaTableWithRelations(String databaseCode, String tableName);

    List<MetaRelation> listMetaRelation(Map<String,Object> condition, PageDesc pageDesc);
    List<MetaRelation> listMetaRelation(String tableId, PageDesc pageDesc);

    List<MetaColumn> listMetaColumns(String tableId, PageDesc pageDesc);

    MetaRelation getMetaRelationById(String relationId);
    MetaRelation getMetaRelationByName(String tableId, String relationName);

    List<MetaRelation> listMetaRelation(String tableId);

    List<MetaColumn> listMetaColumns(String tableId);

    void createRelation(MetaRelation relation);

    void saveRelations(String tableId, List<MetaRelation> relations);

    MetaColumn getMetaColumn(String tableId, String columnName);

    void updateMetaColumn(MetaColumn metaColumn);

    MetaTableCascade getMetaTableCascade(String databaseCode, String tableCode);
}
