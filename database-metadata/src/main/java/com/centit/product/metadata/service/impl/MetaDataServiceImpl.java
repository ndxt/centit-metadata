package com.centit.product.metadata.service.impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.product.metadata.dao.MetaColumnDao;
import com.centit.product.metadata.dao.MetaRelationDao;
import com.centit.product.metadata.dao.MetaTableDao;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.*;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.product.metadata.service.SyncDBPretreatment;
import com.centit.product.metadata.transaction.AbstractDBConnectPools;
import com.centit.product.metadata.utils.TableStoreJsonUtils;
import com.centit.product.metadata.vo.MetaTableCascade;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.UuidOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.database.metadata.*;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.PageDesc;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author zhf
 */
@Service
@Transactional
public class MetaDataServiceImpl implements MetaDataService {
    private static final Logger logger = LoggerFactory.getLogger(MetaDataServiceImpl.class);
    private static final String CONTAIN_SCHEMA = "schema";
    private static final String CONTAIN_ORACLE = "oracle";
    @Autowired
    private SourceInfoDao sourceInfoDao;

    @Autowired
    private MetaTableDao metaTableDao;

    @Autowired
    private MetaColumnDao metaColumnDao;

    @Autowired
    private MetaRelationDao metaRelationDao;

    @Autowired(required = false)
    private SyncDBPretreatment syncDBPretreatment;

    @Override
    public List<SourceInfo> listDatabase(String osId) {
        return sourceInfoDao.listObjectsByProperties(
            CollectionsOpt.createHashMap("osId", osId));
    }

    @Override
    public List<SourceInfo> listDatabase(Map<String, Object> map) {
        return sourceInfoDao.listObjectsByProperties(map);
    }

    @Override
    public JSONArray listMetaTables(Map<String, Object> filterMap, PageDesc pageDesc) {
        return metaTableDao.listObjectsByPropertiesAsJson(filterMap, pageDesc);
    }

    @Override
    public SourceInfo getDatabaseInfo(String databaseCode) {
        return sourceInfoDao.getDatabaseInfoById(databaseCode);
    }

    @Override
    public List<MetaTable> listAllMetaTables(String databaseCode) {
        return metaTableDao.listObjectsByProperties(CollectionsOpt.createHashMap("databaseCode", databaseCode));
    }

    @Override
    public List<MetaTable> listAllMetaTablesWithDetail(String databaseCode) {
        List<MetaTable> metaTables = metaTableDao.listObjectsByProperties(CollectionsOpt.createHashMap("databaseCode", databaseCode));
        for (MetaTable mt : metaTables) {
            metaTableDao.fetchObjectReferences(mt);
            if (mt.getMdRelations() != null) {
                for (MetaRelation mr : mt.getMdRelations()) {
                    metaRelationDao.fetchObjectReferences(mr);
                }
            }
        }
        return metaTables;
    }

    @Override
    public void syncDb(String databaseCode, String recorder, String[] tableNames) {
        List<SimpleTableInfo> dbTables;
        List<MetaTable> metaTables;
        if (tableNames != null) {
            dbTables = getJdbcMetadata(databaseCode, true, tableNames);
            metaTables = metaTableDao.listObjectsByProperties(CollectionsOpt.createHashMap("databaseCode", databaseCode, "tableNames", tableNames));
        } else {
            dbTables = getJdbcMetadata(databaseCode, true, null);
            metaTables = metaTableDao.listObjectsByFilter("where DATABASE_CODE = ?", new Object[]{databaseCode});
        }
        Comparator<TableInfo> comparator = (o1, o2) ->
            StringUtils.compare(o1.getTableName().toUpperCase(), o2.getTableName().toUpperCase());

        Triple<List<SimpleTableInfo>, List<Pair<MetaTable, SimpleTableInfo>>, List<MetaTable>> triple =
            compareMetaBetweenDbTables(metaTables, dbTables, comparator);

        if (triple.getLeft() != null && triple.getLeft().size() > 0) {
            addSyncData(databaseCode, recorder, triple.getLeft());
        }
        if (triple.getRight() != null && triple.getRight().size() > 0) {
            deleteSyncData(triple.getRight());
        }
        if (triple.getMiddle() != null && triple.getMiddle().size() > 0) {
            updateSyncData(recorder, triple.getMiddle());
        }
    }

    @Override
    public void syncSingleTable(String databaseCode, String recorder, String tableName, String tableId){
        List<SimpleTableInfo> dbTables = getJdbcMetadata(databaseCode, true, new String[] {tableName});
        List<MetaTable> metaTables = metaTableDao.listObjectsByProperties(CollectionsOpt.createHashMap(
            "databaseCode", databaseCode, "tableNames", new String[] {tableName}));
        if (dbTables != null && dbTables.size() >0 ) {
            if(metaTables != null && metaTables.size() >0 ){
                updateSyncData(recorder, CollectionsOpt.createList(new MutablePair<>(metaTables.get(0), dbTables.get(0))));
            } else {
                addSyncSingleTable(databaseCode, recorder, dbTables.get(0), tableId);
            }
        } else {
            if(metaTables != null && metaTables.size() >0 ){
                deleteSyncData(metaTables);
            }
        }
    }

    private List<SimpleTableInfo> getJdbcMetadata(String databaseCode, boolean withColumn, String[] tableNames) {
        SourceInfo sourceInfo = sourceInfoDao.getDatabaseInfoById(databaseCode);
        JdbcMetadata jdbcMetadata = new JdbcMetadata();
        try (Connection conn = AbstractDBConnectPools.getDbcpConnect(sourceInfo)) {
            jdbcMetadata.setDBConfig(conn);
            if (sourceInfo.getExtProps().containsKey(CONTAIN_SCHEMA)) {
                jdbcMetadata.setDBSchema(sourceInfo.getExtProps().getString(CONTAIN_SCHEMA).toUpperCase());
            }
            if (sourceInfo.getDatabaseUrl().contains(CONTAIN_ORACLE)) {
                jdbcMetadata.setDBSchema(sourceInfo.getUsername().toUpperCase());
            }
            return jdbcMetadata.listTables(withColumn, tableNames);
        } catch (SQLException e) {
            logger.error("连接数据库【{}】出错", sourceInfo.getDatabaseName());
            throw new ObjectException("连接数据库出错" + e.getMessage());
        }
    }


    @Override
    public List<SimpleTableInfo> listRealTablesWithoutColumn(String databaseCode) {
        List<SimpleTableInfo> dbTableInfo = getJdbcMetadata(databaseCode, false, null);
        dbTableInfo.sort(Comparator.comparing(SimpleTableInfo::getTableType));
        List<MetaTable> metaTables = metaTableDao.listObjectsByProperties(CollectionsOpt.createHashMap("databaseCode", databaseCode));
        Comparator<TableInfo> comparator = (o1, o2) -> StringUtils.compare(o1.getTableName().toUpperCase(), o2.getTableName().toUpperCase());
        Triple<List<SimpleTableInfo>, List<Pair<MetaTable, SimpleTableInfo>>, List<MetaTable>> triple = compareMetaBetweenDbTables(metaTables, dbTableInfo, comparator);
        if (triple.getRight() != null && triple.getRight().size() > 0) {
            for (MetaTable metaTable : triple.getRight()) {
                SimpleTableInfo simpleTableInfo = new SimpleTableInfo();
                simpleTableInfo.setTableName(metaTable.getTableName());
                simpleTableInfo.setTableType("Z");
                dbTableInfo.add(simpleTableInfo);
            }
        }
        return dbTableInfo;
    }

    public static <K, V> Triple<List<K>, List<Pair<V, K>>, List<V>>
    compareMetaBetweenDbTables(List<V> metaTables, List<K> simpleTableInfos, Comparator comparator) {
        if (metaTables == null || metaTables.size() == 0) {
            return new ImmutableTriple<>(
                simpleTableInfos, null, null);
        }
        if (simpleTableInfos == null || simpleTableInfos.size() == 0) {
            return new ImmutableTriple<>(
                null, null, metaTables);
        }
        List<V> oldList = CollectionsOpt.cloneList(metaTables);
        List<K> newList = CollectionsOpt.cloneList(simpleTableInfos);
        oldList.sort(comparator);
        newList.sort(comparator);
        int i = 0;
        int sl = oldList.size();
        int j = 0;
        int dl = newList.size();
        List<K> insertList = new ArrayList<>();
        List<V> delList = new ArrayList<>();
        List<Pair<V, K>> updateList = new ArrayList<>();
        while (i < sl && j < dl) {
            int n = comparator.compare(oldList.get(i), newList.get(j));
            if (n < 0) {
                delList.add(oldList.get(i));
                i++;
            } else if (n == 0) {
                updateList.add(new ImmutablePair<>(oldList.get(i), newList.get(j)));
                i++;
                j++;
            } else {
                insertList.add(newList.get(j));
                j++;
            }
        }
        while (i < sl) {
            delList.add(oldList.get(i));
            i++;
        }
        while (j < dl) {
            insertList.add(newList.get(j));
            j++;
        }
        return new ImmutableTriple<>(insertList, updateList, delList);
    }

    private void addSyncSingleTable(String databaseCode, String recorder, SimpleTableInfo insertNewTable, String tableId) {
        MetaTable metaTable = new MetaTable().convertFromDbTable(insertNewTable);
        metaTable.setDatabaseCode(databaseCode);
        if (metaTable.getTableLabelName() == null || "".equals(metaTable.getTableLabelName())) {
            metaTable.setTableLabelName(insertNewTable.getTableName());
        }
        metaTable.setRecorder(recorder);
        if (tableId != null) {
            metaTable.setTableId(tableId);
        }
        //列 添加 自动填充插件
        if(syncDBPretreatment!=null){
            syncDBPretreatment.pretreatmentTable(metaTable);
        }
        metaTableDao.saveNewObject(metaTable);

        List<SimpleTableField> columns = insertNewTable.getColumns();
        for (SimpleTableField tableField : columns) {
            addSyncSingleTableColumn(recorder, metaTable, tableField);
        }
    }
    private void addSyncData(String databaseCode, String recorder, List<SimpleTableInfo> insertNewTables) {
        for (SimpleTableInfo table : insertNewTables) {
            addSyncSingleTable(databaseCode, recorder, table, null);
        }
    }

    private void addSyncSingleTableColumn(String recorder, MetaTable oldTable, SimpleTableField tableField) {
        MetaColumn metaColumn = new MetaColumn().convertFromTableField(tableField);
        metaColumn.setTableId(oldTable.getTableId());
        metaColumn.setRecorder(recorder);

        if (metaColumn.getFieldLabelName() == null || "".equals(metaColumn.getFieldLabelName())) {
            metaColumn.setFieldLabelName(metaColumn.getColumnName());
        }
        // 添加 自动填充插件
        if(syncDBPretreatment!=null){
            syncDBPretreatment.pretreatmentColumn(metaColumn);
        }
        metaColumnDao.mergeObject(metaColumn);
    }

    private void deleteSyncData(List<MetaTable> deleteTables) {
        for (MetaTable table : deleteTables) {
            metaTableDao.deleteObjectReferences(table);
            metaTableDao.deleteObject(table);
        }
    }

    private void updateSyncData(String recorder,  List<Pair<MetaTable, SimpleTableInfo>> updateTables) {
        for (Pair<MetaTable, SimpleTableInfo> pair : updateTables) {
            MetaTable oldTable = pair.getLeft();
            oldTable.setRecorder(recorder);
            SimpleTableInfo newTable = pair.getRight();
            metaTableDao.updateObject(oldTable.convertFromDbTable(newTable));
            oldTable = metaTableDao.fetchObjectReferences(oldTable);
            List<MetaColumn> oldColumns = oldTable.getColumns();
            List<SimpleTableField> newColumns = newTable.getColumns();
            Comparator<TableField> columnComparator = (o1, o2) -> StringUtils.compare(o1.getColumnName().toUpperCase(), o2.getColumnName().toUpperCase());
            Triple<List<SimpleTableField>, List<Pair<MetaColumn, SimpleTableField>>, List<MetaColumn>> columnCompared = compareMetaBetweenDbTables(oldColumns, newColumns, columnComparator);
            if (columnCompared.getLeft() != null && !columnCompared.getLeft().isEmpty()) {
                //添加 自动填充插件
                addSyncSingleTableColumns(recorder, oldTable, columnCompared);
            }
            if (columnCompared.getRight() != null && !columnCompared.getRight().isEmpty()) {
                deleteSyncSingleTableColumns(columnCompared);
            }
            if (columnCompared.getMiddle() != null && !columnCompared.getMiddle().isEmpty()) {
                //添加 自动填充插件
                updateSyncSingleTableColumns(recorder, columnCompared.getMiddle());
            }
        }
    }

    private void addSyncSingleTableColumns(String recorder, MetaTable oldTable, Triple<List<SimpleTableField>, List<Pair<MetaColumn, SimpleTableField>>, List<MetaColumn>> columnCompared) {
        for (SimpleTableField tableField : columnCompared.getLeft()) {
            addSyncSingleTableColumn(recorder, oldTable, tableField);
        }
    }

    private void deleteSyncSingleTableColumns(Triple<List<SimpleTableField>, List<Pair<MetaColumn, SimpleTableField>>, List<MetaColumn>> columnCompared) {
        for (MetaColumn metaColumn : columnCompared.getRight()) {
            metaColumnDao.deleteObject(metaColumn);
        }
    }

    private void updateSyncSingleTableColumns(String recorder,  List<Pair<MetaColumn, SimpleTableField>>  needUpdateColumns) {
        for (Pair<MetaColumn, SimpleTableField> columnPair : needUpdateColumns) {
            MetaColumn oldColumn = columnPair.getLeft();
            oldColumn.setRecorder(recorder);
            SimpleTableField newColumn = columnPair.getRight();
            oldColumn.convertFromTableField(newColumn);
            //添加 自动填充插件
            if(syncDBPretreatment!=null){
                syncDBPretreatment.pretreatmentColumn(oldColumn);
            }
            metaColumnDao.updateObject(oldColumn);
        }
    }

    @Override
    public void updateMetaTable(MetaTable metaTable) {
        metaTableDao.updateObject(metaTable);
    }

    @Override
    public MetaTable getMetaTable(String tableId) {
        return metaTableDao.getObjectById(tableId);
    }

    @Override
    public MetaTable getMetaTable(String databaseCode, String tableName) {
        return metaTableDao.getMetaTable(databaseCode, tableName);
    }

    private void fetchMetaTableRelations(MetaTable metaTable) {
        metaTableDao.fetchObjectReference(metaTable, "mdColumns");
        metaTableDao.fetchObjectReference(metaTable, "mdRelations");
        if (metaTable != null && metaTable.getMdRelations() != null) {
            for (MetaRelation mr : metaTable.getMdRelations()) {
                metaRelationDao.fetchObjectReference(mr, "relationDetails");
            }
        }
    }

    @Override
    public MetaTable getMetaTableWithRelations(String tableId) {
        MetaTable metaTable = metaTableDao.getObjectById(tableId);
        fetchMetaTableRelations(metaTable);
        return metaTable;
    }

    @Override
    public MetaTable getMetaTableWithRelations(String databaseCode, String tableName) {
        MetaTable metaTable = metaTableDao.getMetaTable(databaseCode, tableName);
        fetchMetaTableRelations(metaTable);
        return metaTable;
    }

    private void fetchMetaRelationDetail(MetaRelation relation) {
        metaRelationDao.fetchObjectReferences(relation);
    }

    @Override
    public MetaRelation getMetaRelationById(String relationId) {
        MetaRelation relation = metaRelationDao.getObjectById(relationId);
        metaRelationDao.fetchObjectReferences(relation);
        return relation;
    }

    @Override
    public List<MetaRelation> listMetaRelation(String tableId) {
        List<MetaRelation> list = metaRelationDao.listObjectsByProperties(
            CollectionsOpt.createHashMap("parentTableId", tableId));
        for (MetaRelation relation : list) {
            fetchMetaRelationDetail(relation);
        }
        return list;
    }

    @Override
    public MetaRelation getMetaRelationByName(String tableId, String relationName) {
        return metaRelationDao.getObjectByProperties(
            CollectionsOpt.createHashMap("parentTableId", tableId,
                "relationName", relationName)
        );
    }

    @Override
    public List<MetaColumn> listMetaColumns(String tableId) {
        return metaColumnDao.listObjectsByProperties(
            CollectionsOpt.createHashMap("tableId", tableId));
    }

    @Override
    public List<MetaRelation> listMetaRelation(Map<String, Object> condition, PageDesc pageDesc) {
        List<MetaRelation> list = metaRelationDao.listObjectsByProperties(
            condition, pageDesc);
        for (MetaRelation relation : list) {
            fetchMetaRelationDetail(relation);
        }
        return list;
    }

    @Override
    public List<MetaRelation> listMetaRelation(String tableId, PageDesc pageDesc) {
        List<MetaRelation> list = metaRelationDao.listObjectsByProperties(
            CollectionsOpt.createHashMap("parentTableId", tableId), pageDesc);
        for (MetaRelation relation : list) {
            fetchMetaRelationDetail(relation);
        }
        return list;
    }

    @Override
    public List<MetaColumn> listMetaColumns(Map<String, Object> condition, PageDesc pageDesc) {
        return metaColumnDao.listObjectsByProperties(
            condition, pageDesc);
    }

    @Override
    public void createRelation(MetaRelation relation) {
        metaRelationDao.saveNewObject(relation);
        metaRelationDao.saveObjectReferences(relation);
    }

    @Override
    public void saveRelations(String tableId, List<MetaRelation> relations) {
        List<MetaRelation> dbRelations = metaRelationDao.listObjectsByProperties(
            CollectionsOpt.createHashMap("parentTableId", tableId));

        Triple<List<MetaRelation>, List<Pair<MetaRelation, MetaRelation>>, List<MetaRelation>> comparedRelation =
            CollectionsOpt.compareTwoList(dbRelations, relations,
                (o1, o2) -> StringUtils.compare(o1.getChildTableId(), o2.getChildTableId()));

        if (comparedRelation.getLeft() != null) {
            //insert
            for (MetaRelation relation : comparedRelation.getLeft()) {
                metaRelationDao.saveNewObject(relation);
                metaRelationDao.saveObjectReference(relation, "relationDetails");
            }
        }

        if (comparedRelation.getRight() != null) {
            //delete
            for (MetaRelation relation : comparedRelation.getRight()) {
                relation = metaRelationDao.fetchObjectReferences(relation);
                metaRelationDao.deleteObject(relation);
                metaRelationDao.deleteObjectReference(relation, "relationDetails");
            }
        }

        if (comparedRelation.getMiddle() != null) {
            //update
            for (Pair<MetaRelation, MetaRelation> pair : comparedRelation.getMiddle()) {
                MetaRelation oldRelation = pair.getLeft();
                oldRelation = metaRelationDao.fetchObjectReference(oldRelation, "relationDetails");
                MetaRelation newRelation = pair.getRight();
                oldRelation.setRelationName(newRelation.getRelationName());
                oldRelation.setRelationComment(newRelation.getRelationComment());
                metaRelationDao.updateObject(oldRelation);

                metaRelationDao.deleteObjectReference(oldRelation, "relationDetails");
                newRelation.setRelationId(oldRelation.getRelationId());
                metaRelationDao.saveObjectReference(newRelation, "relationDetails");
            }
        }
    }

    @Override
    public MetaColumn getMetaColumn(String tableId, String columnName) {
        return metaColumnDao.getObjectById(new MetaColumn(tableId, columnName));
    }

    @Override
    public void updateMetaColumn(MetaColumn metaColumn) {
        metaColumnDao.updateObject(metaColumn);

    }

    @Override
    public MetaTableCascade getMetaTableCascade(String tableId, String token) {
        MetaTableCascade tableCascade = new MetaTableCascade();

        MetaTable metaTable = metaTableDao.getObjectById(tableId);
        tableCascade.setTableInfo(metaTable);
        String tableToken = StringUtils.isBlank(token) ? "T" : token;

        SourceInfo dbInfo = sourceInfoDao.getDatabaseInfoById(metaTable.getDatabaseCode());
        DBType dbType = DBType.mapDBType(dbInfo.getDatabaseUrl());
        tableCascade.setDatabaseType(dbType.toString());
        tableCascade.setTableAlias(tableToken);
        metaTableDao.fetchObjectReferences(metaTable);
        int n = 0;
        for (MetaRelation relation : metaTable.getMdRelations()) {
            String childTableId = relation.getChildTableId();
            MetaTable childTable = metaTableDao.getObjectById(childTableId);

            metaRelationDao.fetchObjectReferences(relation);
            tableCascade.addRelationTable(childTable, relation.getRelationDetails(), tableToken + "_" + n);
            n++;
        }
        tableCascade.setTableFields(metaTable.getMdColumns());

        return tableCascade;
    }

    /**
     * @param databaseCode 数据库
     * @param jsonObject tableStore中的表
     * @param userCode 创建人员
     */
    @Override
    @Transactional
    public void importRelationFromTableStore(String databaseCode, JSONObject jsonObject, String userCode){

        List<MetaRelation> refList = TableStoreJsonUtils.fetchRelations(jsonObject);
        if(refList==null || refList.size()==0)
            return ;
        for(MetaRelation ref : refList){
            MetaTable parentTable = metaTableDao.getMetaTable(databaseCode, ref.getParentTableId());
            if(parentTable==null) continue;
            String parentTableId = parentTable.getTableId();
            MetaTable childTable = metaTableDao.getMetaTable(databaseCode, ref.getChildTableId());
            if(childTable==null) continue;
            String childTableId = childTable.getTableId();

            if(StringUtils.isNotBlank(parentTableId) && StringUtils.isNotBlank(childTableId)) {
                List<MetaRelation> relations = metaRelationDao.listRelationByTables(parentTableId, childTableId);
                if(relations==null || relations.size()==0){ // saveNew
                    String relationId = UuidOpt.getUuidAsString22();
                    ref.setRelationId(relationId);
                    ref.setParentTableId(parentTableId);
                    ref.setChildTableId(childTableId);
                    List<MetaRelDetail> details = ref.getRelationDetails();
                    if(details!=null && details.size()>0) {
                        for (MetaRelDetail detail : details) {
                            detail.setRelationId(relationId);
                        }
                        metaRelationDao.saveNewObject(ref);
                        metaRelationDao.saveObjectReference(ref, "relationDetails");
                    }
                } else if(relations.size()==1){ // update
                    MetaRelation dbRelation = relations.get(0);
                    dbRelation.setRelationName(ref.getRelationName());
                    dbRelation.setRelationComment(ref.getRelationComment());
                    metaRelationDao.updateObject(dbRelation);
                    List<MetaRelDetail> details = ref.getRelationDetails();
                    if(details!=null && details.size()>0) {
                        for (MetaRelDetail detail : details) {
                            detail.setRelationId(dbRelation.getRelationId());
                        }
                        dbRelation.setRelationDetails(details);
                        metaRelationDao.saveObjectReference(dbRelation, "relationDetails");
                    }
                }
            }
        }
    }

}
