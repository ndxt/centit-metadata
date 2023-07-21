package com.centit.product.dbdesign.service.impl;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.product.dbdesign.dao.MetaChangLogDao;
import com.centit.product.dbdesign.dao.PendingMetaColumnDao;
import com.centit.product.dbdesign.dao.PendingMetaTableDao;
import com.centit.product.dbdesign.service.MetaTableManager;
import com.centit.product.metadata.dao.MetaColumnDao;
import com.centit.product.metadata.dao.MetaTableDao;
import com.centit.product.metadata.dao.SourceInfoDao;
import com.centit.product.metadata.po.*;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.product.metadata.service.impl.MetaDataServiceImpl;
import com.centit.product.metadata.transaction.AbstractDruidConnectPools;
import com.centit.product.metadata.utils.PdmTableInfoUtils;
import com.centit.product.metadata.utils.TableStoreJsonUtils;
import com.centit.support.algorithm.*;
import com.centit.support.common.ObjectException;
import com.centit.support.database.ddl.DDLOperations;
import com.centit.support.database.ddl.GeneralDDLOperations;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * MdTable  Service.
 * create by scaffold 2016-06-02
 * <p>
 * 表元数据表状态分为 系统/查询/更新
 * 系统，不可以做任何操作
 * 查询，仅用于通用查询模块，不可以更新
 * 更新，可以更新
 */
@Service
public class MetaTableManagerImpl implements MetaTableManager {

    public static final String VIEW = "V";

    private static final Logger logger = LoggerFactory.getLogger(MetaTableManagerImpl.class);
    @Autowired
    private SourceInfoDao sourceInfoDao;

    @Autowired
    private MetaDataService metaDataService;

    private MetaTableDao metaTableDao;

    @Resource(name = "metaTableDao")
    @NotNull
    public void setMetaTableDao(MetaTableDao baseDao) {
        this.metaTableDao = baseDao;
    }

    @Resource
    private MetaColumnDao metaColumnDao;

    @Resource
    private MetaChangLogDao metaChangLogDao;

    @Resource
    private PendingMetaTableDao pendingMdTableDao;

    @Resource
    private PendingMetaColumnDao pendingMetaColumnDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public JSONArray listMdTablesAsJson(
        String[] fields,
        Map<String, Object> filterMap, PageDesc pageDesc) {

        return metaTableDao.listObjectsByPropertiesAsJson(filterMap, pageDesc);
    }

    @Override
    @Transactional
    public void saveNewPendingMetaTable(PendingMetaTable pmt) {
        pendingMdTableDao.saveNewObject(pmt);
        pendingMdTableDao.saveObjectReferences(pmt);
    }

    @Override
    @Transactional
    public void deletePendingMetaTable(String tableId) {
        pendingMdTableDao.deleteObjectById(tableId);
        Map<String, Object> tempFilter = new HashMap<>();
        tempFilter.put("tableId", tableId);
        pendingMetaColumnDao.deleteObjectsForceByProperties(tempFilter);
    }

    @Override
    @Transactional
    public PendingMetaTable getPendingMetaTable(String tableId) {
        PendingMetaTable resultPdMetaTable = pendingMdTableDao.getObjectById(tableId);
        return pendingMdTableDao.fetchObjectReferences(resultPdMetaTable);
    }

    @Override
    @Transactional
    public MetaChangLog getMetaChangLog(String changeId) {
        return metaChangLogDao.getObjectById(changeId);
    }

    @Override
    @Transactional
    public void savePendingMetaTable(PendingMetaTable pmt) {
        pendingMdTableDao.updateObject(pmt);
        pendingMdTableDao.saveObjectReferences(pmt);
    }


    /**
     * 对比pendingMetaTable和MetaTable中的字段信息，
     * 获取表结构差异对应的Sql语句
     */
    @Override
    @Transactional
    public List<String> makeAlterTableSqlList(String tableId) {
        PendingMetaTable ptable = getPendingMetaTable(tableId);
        if (null == ptable || ("T".equals(ptable.getTableType()) && null == ptable.getColumns())) {
            return null;
        }
        return makeAlterTableSqlList(ptable);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public List<String> makeAlterTableSqlList(PendingMetaTable pendingMetaTable) {
        MetaTable metaTable = metaTableDao.getMetaTable(pendingMetaTable.getDatabaseCode(), pendingMetaTable.getTableName());
        if (metaTable != null) {
            metaTable = metaTableDao.fetchObjectReferences(metaTable);
        }
        SourceInfo sourceInfo = sourceInfoDao.getDatabaseInfoById(pendingMetaTable.getDatabaseCode());
        DBType dbType = DBType.mapDBType(sourceInfo.getDatabaseUrl());
        pendingMetaTable.setDatabaseType(dbType);
        DDLOperations ddlOpt = GeneralDDLOperations.createDDLOperations(dbType);

        List<String> sqlList;
        if (VIEW.equals(pendingMetaTable.getTableType())) {
            sqlList = new ArrayList<>();
            sqlList.add(ddlOpt.makeCreateViewSql(pendingMetaTable.getViewSql(), pendingMetaTable.getTableName()));
        } else {
            sqlList = DDLUtils.makeAlterTableSqlList(pendingMetaTable, metaTable, dbType, ddlOpt);
        }
        return sqlList;
    }

    public void checkPendingMetaTable(PendingMetaTable ptable, String currentUser) {
        if (ptable.isUpdateCheckTimeStamp()) {
            PendingMetaColumn col = ptable.findFieldByName(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP);
            if (col == null) {
                col = new PendingMetaColumn(ptable, MetaTable.UPDATE_CHECK_TIMESTAMP_FIELD);
                col.setFieldLabelName("最新更新时间");
                col.setColumnComment("最新更新时间");
                col.setFieldType(FieldType.DATETIME);
                col.setLastModifyDate(DatetimeOpt.currentUtilDate());
                col.setRecorder(currentUser);
                ptable.addMdColumn(col);
            }
        }
        if ("C".equals(ptable.getTableType())) {
            PendingMetaColumn col = ptable.findFieldByName(MetaTable.OBJECT_AS_CLOB_FIELD);
            if (col == null) {
                col = new PendingMetaColumn(ptable, MetaTable.OBJECT_AS_CLOB_FIELD);
                col.setFieldLabelName("流程实例ID");
                col.setColumnComment("业务对应的工作流程实例ID");
                col.setFieldType(FieldType.JSON_OBJECT);
                col.setMaxLength(32);
                col.setLastModifyDate(DatetimeOpt.currentUtilDate());
                col.setRecorder(currentUser);
                ptable.addMdColumn(col);
            } else { //检查类型 string text FieldType.JSON_OBJECT

            }
        }

        if ("1".equals(ptable.getWorkFlowOptType()) || "2".equals(ptable.getWorkFlowOptType())) {
            PendingMetaColumn col = ptable.findFieldByName(MetaTable.WORKFLOW_INST_ID_PROP);
            if (col == null) {
                col = new PendingMetaColumn(ptable, MetaTable.WORKFLOW_INST_ID_FIELD);
                col.setFieldLabelName("流程实例ID");
                col.setColumnComment("业务对应的工作流程实例ID");
                col.setFieldType(FieldType.STRING);
                col.setMaxLength(32);
                col.setLastModifyDate(DatetimeOpt.currentUtilDate());
                col.setRecorder(currentUser);
                ptable.addMdColumn(col);
            }
        }

        if ("2".equals(ptable.getWorkFlowOptType())) {
            PendingMetaColumn col = ptable.findFieldByName(MetaTable.WORKFLOW_NODE_INST_ID_PROP);
            if (col == null) {
                col = new PendingMetaColumn(ptable, MetaTable.WORKFLOW_NODE_INST_ID_FIELD);
                col.setFieldLabelName("节点实例ID");
                col.setColumnComment("业务对应的工作流节点ID");
                col.setFieldType(FieldType.STRING);
                col.setMaxLength(32);
                col.setLastModifyDate(DatetimeOpt.currentUtilDate());
                col.setRecorder(currentUser);
                ptable.addMdColumn(col);
            }
        }
    }

    /**
     * 对比pendingMetaTable和MetaTable中的字段信息，并对数据库中的表进行重构，
     * 重构成功后将对应的表结构信息同步到 MetaTable中，并在MetaChangeLog中记录信息
     *
     * @return 返回错误编号 和 错误说明， 编号为0表示成功
     */
    @Override
    @Transactional
    public Pair<Integer, String> publishMetaTable(String tableId, String currentUser) {
        try {
            final PendingMetaTable pTable = pendingMdTableDao.getObjectById(tableId);
            pendingMdTableDao.fetchObjectReferences(pTable);
            Pair<Integer, String> ret;
            if (VIEW.equals(pTable.getTableType())) {
                ret = GeneralDDLOperations.checkViewWellDefined(pTable);
                if (StringBaseOpt.isNvl(pTable.getViewSql())) {
                    ret = new ImmutablePair<>(-1, "视图" + pTable.getTableName() + "没有定义sql！");
                }
            } else {
                ret = GeneralDDLOperations.checkTableWellDefined(pTable);
            }
            if (ret.getLeft() != 0) {
                return ret;
            }
            MetaChangLog chgLog = new MetaChangLog();
            List<String> errors = new ArrayList<>();
            SourceInfo mdb = sourceInfoDao.getDatabaseInfoById(pTable.getDatabaseCode());
            DataSourceDescription dbc = DataSourceDescription.valueOf(mdb);

            DBType databaseType = DBType.mapDBType(mdb.getDatabaseUrl());
            pTable.setDatabaseType(databaseType);
            //检查字段定义一致性，包括：检查是否有时间戳、是否和工作流关联
            checkPendingMetaTable(pTable, currentUser);
            List<String> sqlList = TransactionHandler.executeInTransaction(dbc,
                (conn) -> runDdlSql(pTable, errors, conn));
            if (sqlList.size() > 0) {
                chgLog.setDatabaseCode(pTable.getDatabaseCode());
                chgLog.setChangeScript(JSON.toJSONString(sqlList));
                chgLog.setChangeComment(JSON.toJSONString(errors));
                chgLog.setTableID(pTable.getTableId());
                chgLog.setChanger(currentUser);
                metaChangLogDao.saveNewObject(chgLog);
            }
            if (sqlList.size() == 0) {
                return new ImmutablePair<>(2, "信息未变更，无需发布");
            }
            if (errors.size() == 0) {
                pTable.setRecorder(currentUser);
                pTable.setTableState("S");
                pTable.setLastModifyDate(new Date());
                pendingMdTableDao.mergeObject(pTable);
                pendingMdTableDao.saveObjectReferences(pTable);
                if (sqlList.size() > 0) {
                    if (VIEW.equals(pTable.getTableType())) {
                        metaDataService.syncDb(pTable.getDatabaseCode(), currentUser, new String[]{pTable.getTableName()}, pTable.getTableId());
                    } else {
                        pendingToMeta(currentUser, pTable);
                    }
                }
                return new ImmutablePair<>(0, chgLog.getChangeId());
            } else {
                return new ImmutablePair<>(-1, "发布失败!"+JSON.toJSONString(errors));
            }
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e.getMessage());
            return new ImmutablePair<>(-1, "发布失败!" + e.getMessage());
        }
    }

    private List<String> runDdlSql(PendingMetaTable pendingMetaTable, List<String> errors, Connection conn) throws SQLException {
        List<String> sqlList = makeAlterTableSqlList(pendingMetaTable);
        for (String sql : sqlList) {
            try {
                DatabaseAccess.doExecuteSql(conn, sql);
            } catch (SQLException se) {
                errors.add(se.getMessage());
                logger.error("执行sql失败:" + sql, se);
            }
        }
        return sqlList;
    }

    private void pendingToMeta(String currentUser, PendingMetaTable ptable) {
        MetaTable metaTable = metaTableDao.getMetaTable(ptable.getDatabaseCode(), ptable.getTableName());
        if (metaTable != null) {
            metaTable = metaTableDao.getObjectCascadeById(metaTable.getTableId());
            metaTable.setWorkFlowOptType(ptable.getWorkFlowOptType());
            metaTable.setUpdateCheckTimeStamp(ptable.getUpdateCheckTimeStamp());
            metaTable.setRecorder(currentUser);
            metaTable.setRecordDate(new Date());
            metaTableDao.mergeObject(metaTable);
            Set<MetaColumn> setMetaColumn = new HashSet<>();
            setMetaColumn.addAll(metaTable.getMdColumns());
            Set<PendingMetaColumn> setPendingMetaColumn = new HashSet<>();
            setPendingMetaColumn.addAll(ptable.getMdColumns());
            for (MetaColumn m : setMetaColumn) {
                for (PendingMetaColumn p : setPendingMetaColumn) {
                    if (m.getColumnName().equalsIgnoreCase(p.getColumnName())) {
                        m.setColumnLength(p.getMaxLength());
                        m.setFieldLabelName(p.getFieldLabelName());
                        m.setColumnOrder(p.getColumnOrder());
                        m.setPrimaryKey(p.getPrimaryKey());
                        m.setFieldType(p.getFieldType());
                        m.setColumnType(FieldType.mapToDatabaseType(p.getFieldType(), m.getDatabaseType()));
                        m.setScale(p.getScale());
                        m.setMandatory(p.getMandatory());
                        p.setIsCompare(true);
                        m.setIsCompare(true);
                    }
                }
            }
            for (MetaColumn m : setMetaColumn) {
                if (m.getIsCompare() != null && m.getIsCompare()) {
                    metaColumnDao.updateObject(m);
                } else {
                    metaColumnDao.deleteObject(m);
                }
            }
            for (PendingMetaColumn p : setPendingMetaColumn) {
                if (p.getIsCompare() == null || !p.getIsCompare()) {
                    MetaColumn tmp = p.mapToMetaColumn();
                    tmp.setTableId(metaTable.getTableId());
                    metaColumnDao.saveNewObject(tmp);
                }
            }
        } else {
            metaTableDao.saveNewObject(ptable.mapToMetaTable());
            for (PendingMetaColumn p : ptable.getMdColumns()) {
                metaColumnDao.saveNewObject(p.mapToMetaColumn());
            }
        }

    }

    @Override
    @Transactional(readOnly = true)
    public JSONArray listDrafts(String[] fields, Map<String, Object> searchColumn,
                                PageDesc pageDesc) {

        return pendingMdTableDao.listObjectsByPropertiesAsJson(searchColumn, pageDesc);
    }

    @Override
    public List<Pair<String, String>> listTablesInPdm(String pdmFilePath) {
        return PdmTableInfoUtils.listTablesInPdm(pdmFilePath);
    }

    @Override
    @Transactional
    public boolean importTableFromPdm(String pdmFilePath, String tableCode, String databaseCode) {
        PendingMetaTable metaTable = PdmTableInfoUtils.importTableFromPdm(pdmFilePath, tableCode, databaseCode);
        if (metaTable == null) {
            return false;
        }
        pendingMdTableDao.saveNewObject(metaTable);
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    @Transactional
    public List<MetaColumn> getNotInFormFields(String tableId) {
        String sql = "select * from F_META_COLUMN  t where t.table_id= :tableId " +
            "and t.column_name not in " +
            "(select f.column_name from m_model_data_field f join m_meta_form_model m" +
            " on f.model_code=m.model_code and m.table_id = ?)";
        return metaColumnDao.listObjectsBySql(sql, new Object[] {tableId});
    }

    @Override
    public List<MetaColumn> listFields(String tableId) {
        Map<String, Object> filterMap = new HashMap<String, Object>();
        filterMap.put("tableId", tableId);

        return metaColumnDao.listObjectsByProperties(filterMap);
    }

    @Override
    public List<PendingMetaColumn> listMetaColumns(String tableId, PageDesc pageDesc) {
        Map<String, Object> filterMap = new HashMap<String, Object>();
        filterMap.put("tableId", tableId);

        return pendingMetaColumnDao.listObjectsByProperties(filterMap, pageDesc);
    }

    @Override
    public PendingMetaColumn getMetaColumn(String tableId, String columnName) {
        return pendingMetaColumnDao.getObjectById(new MetaColumn(tableId, columnName));
    }

    @Override
    @Transactional
    public Pair<Integer, String> syncPdm(String databaseCode, String pdmFilePath, List<String> tables, String recorder) {
        try {
            List<SimpleTableInfo> pdmTables = PdmTableInfoUtils.importTableFromPdm(pdmFilePath, tables);
            if (pdmTables == null) {
                return new ImmutablePair<>(-1, "读取文件失败,导入失败！");
            }
            List<PendingMetaTable> pendingMetaTables = pendingMdTableDao.listObjectsByFilter("where DATABASE_CODE = ?", new Object[]{databaseCode});
            Comparator<TableInfo> comparator = (o1, o2) -> StringUtils.compare(o1.getTableName().toUpperCase(), o2.getTableName().toUpperCase());
            Triple<List<SimpleTableInfo>, List<Pair<PendingMetaTable, SimpleTableInfo>>, List<PendingMetaTable>> triple = MetaDataServiceImpl.compareMetaBetweenDbTables(pendingMetaTables, pdmTables, comparator);
            if (triple.getLeft() != null && triple.getLeft().size() > 0) {
                //新增
                for (SimpleTableInfo pdmtable : triple.getLeft()) {
                    //表
                    PendingMetaTable metaTable = new PendingMetaTable().convertFromPdmTable(pdmtable);
                    metaTable.setDatabaseCode(databaseCode);
                    metaTable.setRecorder(recorder);
                    pendingMdTableDao.saveNewObject(metaTable);
                    //列
                    List<SimpleTableField> columns = pdmtable.getColumns();
                    for (SimpleTableField field : columns) {
                        PendingMetaColumn mdColumn = new PendingMetaColumn().convertFromTableField(field);
                        mdColumn.setTableId(metaTable.getTableId());
                        mdColumn.setRecorder(recorder);
                        pendingMetaColumnDao.saveNewObject(mdColumn);
                    }
                }
            }
            //通过手动删除的方式，删除pendingMd数据，不进行比较删除
            /*if (triple.getRight() != null && triple.getRight().size() > 0) {
                //删除
                for (PendingMetaTable table : triple.getRight()) {
                    pendingMdTableDao.deleteObjectReferences(table);
                    pendingMdTableDao.deleteObject(table);
                }
            }*/
            if (triple.getMiddle() != null && triple.getMiddle().size() > 0) {
                //更新
                for (Pair<PendingMetaTable, SimpleTableInfo> pair : triple.getMiddle()) {
                    PendingMetaTable oldTable = pair.getLeft();
                    oldTable.setRecorder(recorder);
                    SimpleTableInfo newTable = pair.getRight();
                    //表
                    pendingMdTableDao.updateObject(oldTable.convertFromPdmTable(newTable));
                    //列
                    oldTable = pendingMdTableDao.fetchObjectReferences(oldTable);
                    List<PendingMetaColumn> oldColumns = oldTable.getMdColumns();
                    List<SimpleTableField> newColumns = newTable.getColumns();
                    Comparator<TableField> columnComparator = (o1, o2) -> StringUtils.compare(o1.getColumnName().toUpperCase(), o2.getColumnName().toUpperCase());
                    Triple<List<SimpleTableField>, List<Pair<PendingMetaColumn, SimpleTableField>>, List<PendingMetaColumn>> columnCompared =
                        MetaDataServiceImpl.compareMetaBetweenDbTables(oldColumns, newColumns, columnComparator);
                    if (columnCompared.getLeft() != null && columnCompared.getLeft().size() > 0) {
                        //新增
                        for (SimpleTableField tableField : columnCompared.getLeft()) {
                            PendingMetaColumn metaColumn = new PendingMetaColumn().convertFromTableField(tableField);
                            metaColumn.setTableId(oldTable.getTableId());
                            metaColumn.setRecorder(recorder);
                            pendingMetaColumnDao.saveNewObject(metaColumn);
                        }
                    }
                    if (columnCompared.getRight() != null && columnCompared.getRight().size() > 0) {
                        //删除
                        for (PendingMetaColumn metaColumn : columnCompared.getRight()) {
                            pendingMetaColumnDao.deleteObject(metaColumn);
                        }
                    }
                    if (columnCompared.getMiddle() != null && columnCompared.getMiddle().size() > 0) {
                        //更新
                        for (Pair<PendingMetaColumn, SimpleTableField> columnPair : columnCompared.getMiddle()) {
                            PendingMetaColumn oldColumn = columnPair.getLeft();
                            oldColumn.setRecorder(recorder);
                            SimpleTableField newColumn = columnPair.getRight();
                            pendingMetaColumnDao.updateObject(oldColumn.convertFromTableField(newColumn));
                        }
                    }
                }
            }
            return new ImmutablePair<>(0, "导入成功！");
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e.getMessage());
            return new ImmutablePair<>(-1, "导入失败!" + e.getMessage());
        }
    }

    @Override
    @Transactional
    public Pair<Integer, String> publishDatabase(String databaseCode, String recorder) {
        try {
            List<PendingMetaTable> metaTables = pendingMdTableDao.listObjectsByFilter("where DATABASE_CODE = ? and table_state='W'", new Object[]{databaseCode});
            List<String> success = new ArrayList<>();
            List<String> errors = new ArrayList<>();
            for (PendingMetaTable metaTable : metaTables) {
                metaTable = pendingMdTableDao.fetchObjectReferences(metaTable);
                Pair<Integer, String> ret = GeneralDDLOperations.checkTableWellDefined(metaTable);
                if (ret.getLeft() != 0) {
                    return ret;
                }
                List<String> error = new ArrayList<>();
                SourceInfo mdb = sourceInfoDao.getDatabaseInfoById(metaTable.getDatabaseCode());
                DataSourceDescription dbc = DataSourceDescription.valueOf(mdb);

                DBType databaseType = DBType.mapDBType(mdb.getDatabaseUrl());
                metaTable.setDatabaseType(databaseType);
                checkPendingMetaTable(metaTable, recorder);
                PendingMetaTable finalMetaTable = metaTable;
                List<String> sqls = TransactionHandler.executeInTransaction(dbc,
                    (conn) -> runDdlSql(finalMetaTable, error, conn));
                if (sqls.size() > 0) {
                    success.add(sqls.toString());
                }
                if (error.size() == 0) {
                    metaTable.setRecorder(recorder);
                    metaTable.setTableState("S");
                    metaTable.setLastModifyDate(new Date());
                    pendingMdTableDao.mergeObject(metaTable);
                    pendingMdTableDao.saveObjectReferences(metaTable);
                    if (sqls.size() > 0) {
                        pendingToMeta(recorder, metaTable);
                    }
                } else {
                    errors.add(error.toString());
                }
            }
            MetaChangLog chgLog = new MetaChangLog();
            if (success.size() > 0) {
                chgLog.setDatabaseCode(databaseCode);
                chgLog.setChangeScript(JSON.toJSONString(success));
                chgLog.setChangeComment(JSON.toJSONString(errors));
                chgLog.setChanger(recorder);
                metaChangLogDao.saveNewObject(chgLog);
            }
            if (success.size() == 0) {
                return new ImmutablePair<>(2, "信息未变更，无需批量发布");
            }
            if (errors.size() == 0) {
                return new ImmutablePair<>(0, chgLog.getChangeId());
            } else {
                return new ImmutablePair<>(1, chgLog.getChangeId());
            }
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e.getMessage());
            e.printStackTrace();
            return new ImmutablePair<>(-1, "批量发布失败!" + e.getMessage());
        }
    }

    @Override
    public void updateMetaTable(PendingMetaTable metaTable) {
        pendingMdTableDao.updateObject(metaTable);
    }

    @Override
    public void updateMetaColumn(PendingMetaColumn metaColumn) {
        pendingMetaColumnDao.updateObject(metaColumn);

    }

    @Override
    public MetaTable getMetaTableWithReferences(String tableId) {
        return metaTableDao.getObjectWithReferences(tableId);
    }

    @Override
    public List listCombineTablesByProperty(Map<String, Object> parameters, PageDesc pageDesc) {
        String databaseCode = MapUtils.getString(parameters, "databaseCode");
        String optId = MapUtils.getString(parameters, "optId");
        String osId = MapUtils.getString(parameters, "osId");
        String topUnit = MapUtils.getString(parameters, "topUnit");
        List<Map<String, Object>> mergeTableList = new ArrayList<>();
        if (StringUtils.isNotBlank(databaseCode)) {
            //根据 databaseCode查询表信息
            mergeTableList = listCombineTables(parameters);
        } else if (StringUtils.isNotBlank(optId) || StringUtils.isNotBlank(osId)) {
            //根据optId查询表信息
            JSONArray metaTablesJsonArray = metaTableDao.getMetaTableListWithTableOptRelation(parameters);
            JSONArray pendingMetaTableJSONArray = pendingMdTableDao.getPendingMetaTableListWithTableOptRelation(parameters);
            mergeTableList = mergeTableDataList(JSONArray.parseArray(JSON.toJSONString(metaTablesJsonArray), Map.class),
                JSONArray.parseArray(JSON.toJSONString(pendingMetaTableJSONArray), Map.class));
        } else if (StringUtils.isNotBlank(topUnit)) {
            //根据topUnit查询表信息
            JSONArray metaTablesJsonArray = metaTableDao.getMetaTableList(parameters);
            JSONArray pendingMetaTableJSONArray = pendingMdTableDao.getPendingMetaTableList(parameters);
            mergeTableList = mergeTableDataList(JSONArray.parseArray(JSON.toJSONString(metaTablesJsonArray), Map.class),
                JSONArray.parseArray(JSON.toJSONString(pendingMetaTableJSONArray), Map.class));
        }
        pageDesc.setTotalRows(mergeTableList.size());
        if (CollectionUtils.sizeIsEmpty(mergeTableList)) {
            return Collections.emptyList();
        }
        pageDesc.setTotalRows(mergeTableList.size());
        sortCombineTables(parameters, mergeTableList);
        return pagination(mergeTableList, pageDesc.getPageNo(), pageDesc.getPageSize());
    }

    @Override
    public boolean isTableExist(String tableName, String dataBaseCode) {
        return pendingMdTableDao.isTableExist(tableName, dataBaseCode)
            || metaTableDao.isTableExist(tableName, dataBaseCode);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void syncDb(String databaseCode, String userCode, String[] tableNames) {
        SourceInfo databaseInfo = metaDataService.getDatabaseInfo(databaseCode);
        if (databaseInfo != null && !"D".equals(databaseInfo.getSourceType())) {
            throw new ObjectException("选择的资源不支持反向工程！");
        }
        metaDataService.syncDb(databaseCode, userCode, tableNames);
        deletePendingTableWithColumns(databaseCode, tableNames, userCode);
    }

    @Override
    public PendingMetaTable initPendingMetaTable(String tableId, String userCode) {
        MetaTable metaTable = this.getMetaTableWithReferences(tableId);
        if (null == metaTable) {
            throw new ObjectException("tableId有误!");
        }
        PendingMetaTable pendingMetaTable = new PendingMetaTable();
        BeanUtils.copyProperties(metaTable, pendingMetaTable);
        pendingMetaTable.setRecorder(userCode);
        pendingMetaTable.setTableState("S");
        List<PendingMetaColumn> mdColumns = new ArrayList<>();
        List<MetaColumn> metaColumns = metaTable.getColumns();
        for (MetaColumn metaColumn : metaColumns) {
            PendingMetaColumn pendingMetaColumn = new PendingMetaColumn();
            BeanUtils.copyProperties(metaColumn, pendingMetaColumn);
            mdColumns.add(pendingMetaColumn);
        }
        pendingMetaTable.setMdColumns(mdColumns);
        this.saveNewPendingMetaTable(pendingMetaTable);
        return pendingMetaTable;
    }

    /**
     * 删除 f_pending_meta_table 和 f_pending_meta_column 中的数据
     *
     * @param databaseCode 数据库code
     * @param tableNames   表名
     */
    private void deletePendingTableWithColumns(String databaseCode, String[] tableNames, String userCode) {
        Map<String, Object> filterMap = CollectionsOpt.createHashMap("databaseCode", databaseCode, "tableType_ne", "V");
        if (tableNames != null) {
            filterMap.put("tableNames", tableNames);
        }
        List<MetaTable> metaTables = metaTableDao.listObjectsByProperties(filterMap);
        if (!CollectionUtils.sizeIsEmpty(metaTables)) {
            String[] tableIds = CollectionsOpt.listToArray(metaTables.stream().map(MetaTable::getTableId).collect(Collectors.toSet()));
            Map<String, Object> deleteFilterMap = CollectionsOpt.createHashMap("tableId_in", tableIds);
            pendingMdTableDao.deleteObjectsByProperties(deleteFilterMap);
            pendingMetaColumnDao.deleteObjectsByProperties(deleteFilterMap);
            for (String tableId : tableIds) {
                initPendingMetaTable(tableId, userCode);
            }
        }
    }

    /**
     * state	编辑		重构		发布  f_md_table  f_pending_meta_table
     * NEW		 ×			√			√       ×             √
     * UPDATE	 √      	√			√       √             √(state:W)
     * RELEASED √ 			√ 			×       √              √(state:S)
     * UNCHANGED √			√			×        √             ×
     *
     * @param mcMaps
     * @param pmcMaps
     * @return
     */
    private List<Map<String, Object>> mergeTableDataList(List<Map> mcMaps, List<Map> pmcMaps) {
        Comparator<Map> comparator = (o1, o2) -> StringUtils.compare((MapUtils.getString(o1, "tableName") + MapUtils.getString(o1, "databaseCode")).toLowerCase(),
            (MapUtils.getString(o2, "tableName") + MapUtils.getString(o2, "databaseCode")).toLowerCase());
        Triple<List<Map>, List<Pair<Map, Map>>, List<Map>> listTriple = CollectionsOpt.compareTwoList(mcMaps, pmcMaps, comparator);
        //mcMaps不存在pmcMaps存在  state NEW  新建状态 不展示编辑按钮，展示重构，发布按钮
        List<Map> left = listTriple.getLeft();
        //mcMaps,pmcMaps都存在  state为UPDATE,RELEASED   更新状态展示编辑，重构，发布按钮
        List<Pair<Map, Map>> middle = listTriple.getMiddle();
        //mcMaps存在pmcMaps不存在 state为 UNCHANGED 展示编辑。重构,发布按钮不展示
        List<Map> right = listTriple.getRight();

        List<Map<String, Object>> resultMaps = new ArrayList<>();
        if (!CollectionUtils.sizeIsEmpty(left)) {
            for (Map map : left) {
                map.put("state", "NEW");
                resultMaps.add(map);
            }
        }
        if (!CollectionUtils.sizeIsEmpty(right)) {
            for (Map map : right) {
                map.put("state", "UNCHANGED");
                resultMaps.add(map);
            }
        }
        if (!CollectionUtils.sizeIsEmpty(middle)) {
            for (Pair<Map, Map> pair : middle) {
                //mcMaps的子集
                Map leftPair = pair.getLeft();
                //pmcMaps的子集
                Map rightPair = pair.getRight();
                //如果不相等，以左边为基准
                if ("S".equals(MapUtils.getString(rightPair, "tableState"))) {
                    leftPair.put("state", "RELEASED");
                } else {
                    leftPair.put("state", "UPDATE");
                }
                resultMaps.add(leftPair);
            }
        }
        return resultMaps;
    }

    /**
     * 对combineTables数据进行排序
     *
     * @param filterMap  排序需要的key
     * @param resultMaps 需要被排序的集合
     */
    private void sortCombineTables(Map<String, Object> filterMap, List<Map<String, Object>> resultMaps) {
        String[] sortKey = {"tableType", "tableName"};
        Comparator<Map> comparator = (a, b) ->
        {
            for (String sort : sortKey) {
                int cr= GeneralAlgorithm.compareTwoObject(a.get(sort), b.get(sort));
                if (cr != 0) {
                    return cr;
                }
            }
            return 0;
        };
        resultMaps.sort(comparator);
    }

    /**
     * <p>Description: 内存分页 </p>
     *
     * @param records  待分页的数据
     * @param pageNum  当前页码
     * @param pageSize 每页显示的条数
     * @return 分页之后的数据
     */
    private <T> List<T> pagination(List<T> records, int pageNum, int pageSize) {
        if (CollectionUtils.isEmpty(records)) {
            return Collections.emptyList();
        }
        int pageNumCopy = pageNum;
        if (0 == pageNum) {
            pageNumCopy = 1;
        }
        if (pageNumCopy < 0 || pageSize < 0) {
            return Collections.emptyList();
        }
        return records.stream().skip((pageNumCopy - 1) * pageSize).limit(pageSize).collect(Collectors.toList());
    }

    /**
     * 获取MetaTable和PendingMetaTable组合后的数据
     *
     * @param filerMap
     * @return
     */
    private List<Map<String, Object>> listCombineTables(Map<String, Object> filerMap) {
        List<MetaTable> metaTables = metaTableDao.listObjectsByProperties(filerMap);
        List<PendingMetaTable> pendingMetaTables = pendingMdTableDao.listObjectsByProperties(filerMap);
        if (CollectionUtils.sizeIsEmpty(metaTables) && CollectionUtils.sizeIsEmpty(pendingMetaTables)) {
            return Collections.EMPTY_LIST;
        }
        List<Map> metaTablesMaps = JSONArray.parseArray(JSON.toJSONString(metaTables), Map.class);
        List<Map> pendingMetaTablesMaps = JSONArray.parseArray(JSON.toJSONString(pendingMetaTables), Map.class);
        return mergeTableDataList(metaTablesMaps, pendingMetaTablesMaps);
    }

    @Override
    public JSONArray viewList(String databaseId, String sql) throws SQLException, IOException {
        SourceInfo sourceInfo = sourceInfoDao.getDatabaseInfoById(databaseId);
        try (Connection conn = AbstractDruidConnectPools.getDbcpConnect(sourceInfo)) {
            return DatabaseAccess.findObjectsAsJSON(conn, sql, null, 1, 10);
        }
    }

    @Override
    @Transactional
    public void importFromTableStore(String databaseCode, JSONObject jsonObject, String userCode) {
        List<PendingMetaTable> tableList = TableStoreJsonUtils.fetchTables(jsonObject);
        if(tableList==null || tableList.size()==0)
            return ;
        Map<String, String> tableIdMap = new HashMap<>();
        //逐个保存 表信息
        for(PendingMetaTable table : tableList){
            PendingMetaTable dbTable = pendingMdTableDao.getTableByName(table.getTableName(), databaseCode);
            if(dbTable != null){
                tableIdMap.put(table.getTableId(), dbTable.getTableId());
                dbTable.setTableType(table.getTableType());
                //dbTable.setTableName(table.getTableName());
                dbTable.setTableLabelName(table.getTableLabelName());
                dbTable.setTableComment(table.getTableComment());
                dbTable.setViewSql(table.getViewSql());
                dbTable.setTableState("W");
                // 字段
                List<PendingMetaColumn> columns = table.getMdColumns();
                if(columns!=null && columns.size()>0){
                    for(PendingMetaColumn col : columns){
                        col.setTableId(dbTable.getTableId());
                    }
                }
                dbTable.setMdColumns(columns);
                pendingMdTableDao.updateObject(dbTable);
                pendingMdTableDao.saveObjectReference(dbTable, "mdColumns");
            } else {
                String newTalbeId = UuidOpt.getUuidAsString22();
                tableIdMap.put(table.getTableId(), newTalbeId);
                table.setDatabaseCode(databaseCode);
                table.setTableState("W");
                table.setRecorder(userCode);
                // 字段
                List<PendingMetaColumn> columns = table.getMdColumns();
                if(columns!=null && columns.size()>0){
                    for(PendingMetaColumn col : columns){
                        col.setTableId(newTalbeId);
                    }
                }
                pendingMdTableDao.saveNewObject(table);
                pendingMdTableDao.saveObjectReference(table, "mdColumns");
            }
        }
    }

}

