package com.centit.product.dbdesign.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.framework.jdbc.dao.DatabaseOptUtils;
import com.centit.framework.jdbc.service.BaseEntityManagerImpl;
import com.centit.product.dbdesign.dao.MetaChangLogDao;
import com.centit.product.dbdesign.dao.PendingMetaColumnDao;
import com.centit.product.dbdesign.dao.PendingMetaTableDao;
import com.centit.product.dbdesign.pdmutils.PdmTableInfoUtils;
import com.centit.product.dbdesign.po.MetaChangLog;
import com.centit.product.dbdesign.po.PendingMetaColumn;
import com.centit.product.dbdesign.po.PendingMetaTable;
import com.centit.product.dbdesign.service.MetaTableManager;
import com.centit.product.metadata.dao.MetaColumnDao;
import com.centit.product.metadata.dao.MetaTableDao;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.impl.MetaDataServiceImpl;
import com.centit.support.algorithm.DatetimeOpt;
import com.centit.support.database.ddl.*;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.jsonmaptable.JsonObjectDao;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import javax.annotation.Resource;
import javax.validation.constraints.NotNull;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

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
public class MetaTableManagerImpl
    extends BaseEntityManagerImpl<MetaTable, String, MetaTableDao>
    implements MetaTableManager {

    public static final Log log = LogFactory.getLog(MetaTableManager.class);


    private MetaTableDao metaTableDao;

    @Resource(name = "metaTableDao")
    @NotNull
    public void setMetaTableDao(MetaTableDao baseDao) {
        this.metaTableDao = baseDao;
        setBaseDao(this.metaTableDao);
    }

    @Resource
    private MetaColumnDao metaColumnDao;

    @Resource
    private MetaChangLogDao metaChangLogDao;

    @Resource
    private PendingMetaTableDao pendingMdTableDao;

    @Resource
    private PendingMetaColumnDao pendingMetaColumnDao;

    @Resource
    protected IntegrationEnvironment integrationEnvironment;

    /*
         @PostConstruct
        public void init() {

        }

     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public JSONArray listMdTablesAsJson(
        String[] fields,
        Map<String, Object> filterMap, PageDesc pageDesc) {

        return baseDao.listObjectsAsJson(filterMap, pageDesc);
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
        MetaChangLog metaChangLog = metaChangLogDao.getObjectById(changeId);
        return metaChangLog;
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
    public List<String> makeAlterTableSqls(String tableId) {
        PendingMetaTable ptable = getPendingMetaTable(tableId);

        /*PendingMetaTable ptable = pendingMdTableDao.getObjectById(tableId);

        Set<PendingMetaColumn> pColumn =
            new HashSet<>(pendingMetaColumnDao.listObjectsByProperty("tableId", tableId));
        Set<PendingMetaRelation> pRelation =
            new HashSet<>(pendingRelationDao.listObjectsByProperty("parentTableId", tableId));

        Iterator<PendingMetaRelation> itr = pRelation.iterator();
        while (itr.hasNext()) {
            PendingMetaRelation relation = itr.next();
            Set<PendingMetaRelDetail> relDetails = new HashSet<>(
                pendingMetaRelDetialDao.listObjectsByProperty("relationId", relation.getRelationId()));
            relation.setRelationDetails(relDetails);
        }

        ptable.setMdColumns(pColumn);
        ptable.setMdRelations(pRelation);*/

        return makeAlterTableSqls(ptable);
    }

    @Transactional
    public List<String> makeAlterTableSqls(PendingMetaTable ptable) {
        MetaTable stable = metaTableDao.getMetaTable(ptable.getDatabaseCode(),ptable.getTableName());
        if(stable!=null)
          stable = metaTableDao.fetchObjectReferences(stable);

        DatabaseInfo mdb = integrationEnvironment.getDatabaseInfo(ptable.getDatabaseCode());
        //databaseInfoDao.getDatabaseInfoById(ptable.getDatabaseCode());

        DBType dbType = DBType.mapDBType(mdb.getDatabaseUrl());
        ptable.setDatabaseType(dbType);
        DDLOperations ddlOpt = null;
        switch (dbType) {
            case Oracle:
                ddlOpt = new OracleDDLOperations();
                break;
            case DB2:
                ddlOpt = new DB2DDLOperations();
                break;
            case SqlServer:
                ddlOpt = new SqlSvrDDLOperations();
                break;
            case MySql:
                ddlOpt = new MySqlDDLOperations();
                break;
            case PostgreSql:
                ddlOpt = new PostgreSqlDDLOperations();
                break;
            default:
                ddlOpt = new OracleDDLOperations();
                break;
        }

        List<String> sqls = new ArrayList<>();
        if (stable == null) {
            sqls.add(ddlOpt.makeCreateTableSql(ptable));
        } else {
            stable.setDatabaseType(dbType);
            for (PendingMetaColumn pcol : ptable.getMdColumns()) {
                MetaColumn ocol = stable.findFieldByColumn(pcol.getColumnName());
                if (ocol == null) {
                    sqls.add(ddlOpt.makeAddColumnSql(
                        ptable.getTableName(), pcol));
                } else {
                    if (pcol.getColumnType().equalsIgnoreCase(ocol.getColumnType())) {
                        if (!pcol.getMaxLength().equals(ocol.getMaxLength()) ||
                            !pcol.getScale().equals(ocol.getScale())) {
                            sqls.add(ddlOpt.makeModifyColumnSql(
                                ptable.getTableName(), ocol, pcol));
                        }
                    } else {
                        sqls.addAll(ddlOpt.makeReconfigurationColumnSqls(
                            ptable.getTableName(), ocol.getColumnName(), pcol));
                    }
                }
            }

            for (MetaColumn ocol : stable.getMdColumns()) {
                PendingMetaColumn pcol = ptable.findFieldByColumn(ocol.getColumnName());
                if (pcol == null) {
                    sqls.add(ddlOpt.makeDropColumnSql(stable.getTableName(), ocol.getColumnName()));
                }
            }
        }

        return sqls;
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
        //TODO 根据不同的表类别 做不同的重构
        try {
            PendingMetaTable ptable = pendingMdTableDao.getObjectById(tableId);
            ptable = pendingMdTableDao.fetchObjectReferences(ptable);

            Pair<Integer, String> ret = GeneralDDLOperations.checkTableWellDefined(ptable);
            if (ret.getLeft() != 0)
                return ret;
            MetaChangLog chgLog = new MetaChangLog();
            List<String> errors = new ArrayList<>();

            DatabaseInfo mdb = integrationEnvironment.getDatabaseInfo(ptable.getDatabaseCode());
            //databaseInfoDao.getDatabaseInfoById(ptable.getDatabaseCode());

            DataSourceDescription dbc = new DataSourceDescription();
            dbc.setDatabaseCode(mdb.getDatabaseCode());
            dbc.setConnUrl(mdb.getDatabaseUrl());
            dbc.setUsername(mdb.getUsername());
            dbc.setPassword(mdb.getClearPassword());
            Connection conn = DbcpConnectPools.getDbcpConnect(dbc);

            DBType databaseType = DBType.mapDBType(conn);
            ptable.setDatabaseType(databaseType);
            JsonObjectDao jsonDao = GeneralJsonObjectDao.createJsonObjectDao(conn);
            //检查字段定义一致性，包括：检查是否有时间戳、是否和工作流关联
            checkPendingMetaTable(ptable, currentUser);
            List<String> sqls=getStringsSql(ptable, errors, conn, jsonDao);
            if (sqls.size() > 0) {
                chgLog.setDatabaseCode(ptable.getDatabaseCode());
                chgLog.setChangeScript(JSON.toJSONString(sqls));
                chgLog.setChangeComment(JSON.toJSONString(errors));
                //chgLog.setChangeId(String.valueOf(metaChangLogDao.getNextKey()));
                chgLog.setTableID(ptable.getTableId());
                chgLog.setChanger(currentUser);
                metaChangLogDao.saveNewObject(chgLog);
            }
            if (sqls.size() == 0)
                return new ImmutablePair<>(2, "信息未变更，无需发布");
            if (errors.size() == 0) {
                ptable.setRecorder(currentUser);
                ptable.setTableState("S");
                ptable.setLastModifyDate(new Date());
                pendingMdTableDao.mergeObject(ptable);
                pendingMdTableDao.saveObjectReferences(ptable);
                if (sqls.size() > 0) {
                    pendingToMeta(currentUser, ptable);
                }
                return new ImmutablePair<>(0, chgLog.getChangeId());
            } else
                return new ImmutablePair<>(1, chgLog.getChangeId());
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e.getMessage());
            return new ImmutablePair<>(-1, "发布失败!" + e.getMessage());
        }
    }

    private List<String> getStringsSql(PendingMetaTable ptable, List<String> errors, Connection conn, JsonObjectDao jsonDao) throws SQLException {
        List<String> sqls;
        try {
             sqls = makeAlterTableSqls(ptable);
            for (String sql : sqls) {
                try {
                    jsonDao.doExecuteSql(sql);
                } catch (SQLException se) {
                    errors.add(se.getMessage());
                    logger.error("执行sql失败:" + sql, se);
                }
            }
        } finally {
            conn.close();
        }
        return sqls;
    }

    private void pendingToMeta(String currentUser, PendingMetaTable ptable) {
        MetaTable metaTable = metaTableDao.getMetaTable(ptable.getDatabaseCode(),ptable.getTableName());
        if (metaTable!=null) {
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
                if (m.getIsCompare()) {
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
        } else{
            metaTableDao.saveNewObject(ptable.mapToMetaTable());
            for(PendingMetaColumn p:ptable.getMdColumns()){
                metaColumnDao.saveNewObject(p.mapToMetaColumn());
            }
        }

    }

    @Override
    @Transactional(readOnly = true)
    public JSONArray listDrafts(String[] fields, Map<String, Object> searchColumn,
                                PageDesc pageDesc) {

        JSONArray listTables =
            pendingMdTableDao.listObjectsAsJson(searchColumn, pageDesc);
        return listTables;
    }

    @Override
    public List<Pair<String, String>> listTablesInPdm(String pdmFilePath) {
        return PdmTableInfoUtils.listTablesInPdm(pdmFilePath);
    }

    @Override
    @Transactional
    public boolean importTableFromPdm(String pdmFilePath, String tableCode, String databaseCode) {
        PendingMetaTable metaTable = PdmTableInfoUtils.importTableFromPdm(pdmFilePath, tableCode, databaseCode);
        if (metaTable == null)
            return false;
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
            " on f.model_code=m.model_code and m.table_id=:tableId  )";
        return metaColumnDao.listObjectsBySql(sql,
            QueryUtils.createSqlParamsMap("tableId", tableId));
    }

    @Override
    public List<MetaColumn> listFields(String tableId) {
        Map<String, Object> filterMap = new HashMap<String, Object>();
        filterMap.put("tableId", tableId);

        return metaColumnDao.listObjects(filterMap);
    }

    @Override
    public List<PendingMetaColumn> listMetaColumns(String tableId, PageDesc pageDesc) {
        Map<String, Object> filterMap = new HashMap<String, Object>();
        filterMap.put("tableId", tableId);

        return pendingMetaColumnDao.listObjectsByProperties(filterMap,pageDesc);
    }

    @Override
    public PendingMetaColumn getMetaColumn(String tableId, String columnName) {
        return pendingMetaColumnDao.getObjectById(new MetaColumn(tableId, columnName));
    }

    @Override
    @Transactional
    public Pair<Integer, String> syncPdm(String databaseCode, String pdmFilePath, List<String> tables, String recorder) {
        try {
            List<SimpleTableInfo> pdmTables = PdmTableInfoUtils.importTableFromPdm(pdmFilePath,tables);
            if (pdmTables == null)
                return new ImmutablePair<>(-1, "读取文件失败,导入失败！");
            List<PendingMetaTable> pendingMetaTables = pendingMdTableDao.listObjectsByFilter("where DATABASE_CODE = ?", new Object[]{databaseCode});
            Comparator<TableInfo> comparator = (o1, o2) -> StringUtils.compare(o1.getTableName().toUpperCase(), o2.getTableName().toUpperCase());
            Triple<List<SimpleTableInfo>, List<Pair<PendingMetaTable, SimpleTableInfo>>, List<PendingMetaTable>> triple = MetaDataServiceImpl.compareMetaBetweenDbTables(pendingMetaTables,pdmTables,comparator);
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
            if (triple.getRight() != null && triple.getRight().size() > 0) {
                //删除
                for (PendingMetaTable table : triple.getRight()) {
                    pendingMdTableDao.deleteObjectReferences(table);
                    pendingMdTableDao.deleteObject(table);
                }
            }
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
                    List<PendingMetaColumn> oldColumns = oldTable.getColumns();
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
    public Pair<Integer, String> publishDatabase(String databaseCode, String recorder){
        try {
            List<PendingMetaTable> metaTables = pendingMdTableDao.listObjectsByFilter("where DATABASE_CODE = ? and table_state='W'", new Object[]{databaseCode});
            List<String> success = new ArrayList<>();
            List<String> errors = new ArrayList<>();
            for (PendingMetaTable metaTable : metaTables) {
                metaTable = pendingMdTableDao.fetchObjectReferences(metaTable);

                Pair<Integer, String> ret = GeneralDDLOperations.checkTableWellDefined(metaTable);
                if (ret.getLeft() != 0)
                    return ret;
                List<String> error = new ArrayList<>();
                DatabaseInfo mdb = integrationEnvironment.getDatabaseInfo(metaTable.getDatabaseCode());
                DataSourceDescription dbc = new DataSourceDescription();
                dbc.setDatabaseCode(mdb.getDatabaseCode());
                dbc.setConnUrl(mdb.getDatabaseUrl());
                dbc.setUsername(mdb.getUsername());
                dbc.setPassword(mdb.getClearPassword());
                Connection conn = DbcpConnectPools.getDbcpConnect(dbc);
                DBType databaseType = DBType.mapDBType(conn);
                metaTable.setDatabaseType(databaseType);
                JsonObjectDao jsonDao = GeneralJsonObjectDao.createJsonObjectDao(conn);
                //检查字段定义一致性，包括：检查是否有时间戳、是否和工作流关联
                checkPendingMetaTable(metaTable, recorder);
                List<String> sqls = getStringsSql(metaTable, error, conn, jsonDao);
                if (sqls.size() > 0)
                    success.add(sqls.toString());
                if (error.size() == 0) {
                    metaTable.setRecorder(recorder);
                    metaTable.setTableState("S");
                    metaTable.setLastModifyDate(new Date());
                    pendingMdTableDao.mergeObject(metaTable);
                    pendingMdTableDao.saveObjectReferences(metaTable);
                    if (sqls.size() > 0) {
                        pendingToMeta(recorder,metaTable);
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
                chgLog.setChangeId(String.valueOf(metaChangLogDao.getNextKey()));
                chgLog.setChanger(recorder);
                metaChangLogDao.saveNewObject(chgLog);
            }
            if (success.size() == 0)
                return new ImmutablePair<>(2, "信息未变更，无需批量发布");
            if (errors.size() == 0)
                return new ImmutablePair<>(0, chgLog.getChangeId());
            else
                return new ImmutablePair<>(1, chgLog.getChangeId());
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e.getMessage());
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
}

