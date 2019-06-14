package com.centit.product.dbdesign.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.framework.jdbc.service.BaseEntityManagerImpl;
import com.centit.product.dbdesign.dao.*;
import com.centit.product.dbdesign.pdmutils.PdmTableInfo;
import com.centit.product.dbdesign.po.*;
import com.centit.product.dbdesign.service.MetaTableManager;
import com.centit.product.metadata.dao.MetaColumnDao;
import com.centit.product.metadata.dao.MetaRelationDao;
import com.centit.product.metadata.dao.MetaTableDao;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.product.metadata.po.MetaTable;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.DatetimeOpt;
import com.centit.support.database.ddl.*;
import com.centit.support.database.jsonmaptable.*;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.utils.*;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
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
        MetaTable stable = metaTableDao.getObjectById(ptable.getTableId());
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
                    if (pcol.getColumnType().equals(ocol.getColumnType())) {
                        if (pcol.getMaxLength() != ocol.getMaxLength() ||
                            pcol.getScale() != ocol.getScale()) {
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
        if ("Y".equals(ptable.getUpdateCheckTimeStamp())) {
            PendingMetaColumn col = ptable.findFieldByName(MetaTable.UPDATE_CHECK_TIMESTAMP_PROP);
            if (col == null) {
                col = new PendingMetaColumn(ptable, MetaTable.UPDATE_CHECK_TIMESTAMP_FIELD);
                col.setFieldLabelName("最新更新时间");
                col.setColumnComment("最新更新时间");
                col.setColumnFieldType(FieldType.DATETIME);
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
                col.setColumnFieldType(FieldType.INTEGER);
                col.setMaxLengthM(12);
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
                col.setColumnFieldType(FieldType.INTEGER);
                col.setMaxLengthM(12);
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
            List<String> sqls = makeAlterTableSqls(ptable);
            try {
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
            chgLog.setChangeScript(JSON.toJSONString(sqls));
            chgLog.setChangeComment(JSON.toJSONString(errors));
            chgLog.setChangeId(String.valueOf(metaChangLogDao.getNextKey()));
            chgLog.setTableID(ptable.getTableId());
            chgLog.setChanger(currentUser);
            metaChangLogDao.saveNewObject(chgLog);
            if (errors.size() == 0) {
                ptable.setRecorder(currentUser);
                ptable.setTableState("S");
                ptable.setLastModifyDate(new Date());
                pendingMdTableDao.mergeObject(ptable);
                if (sqls.size() > 0) {
                    MetaTable table = ptable.mapToMetaTable(); //new MetaTable(ptable)
                    metaTableDao.mergeObject(table);

                    List<MetaColumn> metaColumns = table.getColumns();
                    Map<String, Object> cFilter = new HashMap<>();
                    cFilter.put("tableId", table.getTableId());
                    metaColumnDao.deleteObjectsByProperties(cFilter);
                    if (metaColumns != null && metaColumns.size() > 0) {
                        for (MetaColumn metaColumn : metaColumns) {
                            metaColumnDao.saveNewObject(metaColumn);
                        }
                    }
                }
                return new ImmutablePair<>(0, "发布成功！");
            } else
                return new ImmutablePair<>(-10, "发布失败!" + JSON.toJSONString(errors));
        } catch (Exception e) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            logger.error(e.getMessage());
            return new ImmutablePair<>(-1, "发布失败!" + e.getMessage());
        }
    }

    @Override
    @Transactional(readOnly = true)
    public JSONArray listDrafts(String[] fields, Map<String, Object> searchColumn,
                                PageDesc pageDesc) {

        JSONArray listTables =
            pendingMdTableDao.listObjectsAsJson(searchColumn, pageDesc);

        List<DatabaseInfo> databases = integrationEnvironment.listDatabaseInfo();
        for (Object obj : listTables) {
            JSONObject table = (JSONObject) obj;
            String databaseCode = table.getString("databaseCode");
            if (databaseCode != null) {
                for (DatabaseInfo di : databases) {
                    if (databaseCode.equals(di.getDatabaseCode())) {
                        table.put("databaseName", di.getDatabaseName());
                        break;
                    }
                }
            }
        }
        return listTables;
    }

    @Override
    public List<Pair<String, String>> listTablesInPdm(String pdmFilePath) {
        return PdmTableInfo.listTablesInPdm(pdmFilePath);
    }

    @Override
    @Transactional
    public boolean importTableFromPdm(String pdmFilePath, String tableCode, String databaseCode) {
        PendingMetaTable metaTable = PdmTableInfo.importTableFromPdm(pdmFilePath, tableCode, databaseCode);
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
    @Transactional
    public Pair<Integer, String> syncPdm(String databaseCode, String pdmFilePath, List<String> tables, String recorder) {
        try {
            List<SimpleTableInfo> pdmTables = PdmTableInfo.importTableFromPdm(pdmFilePath,tables);
            if (pdmTables == null)
                return new ImmutablePair<>(-1, "读取文件失败,导入失败！");
            List<PendingMetaTable> pendingMetaTables = pendingMdTableDao.listObjectsByFilter("where DATABASE_CODE = ?", new Object[]{databaseCode});
            Comparator<TableInfo> comparator = (o1, o2) -> StringUtils.compare(o1.getTableName(), o2.getTableName());
            Triple<List<SimpleTableInfo>, List<Pair<PendingMetaTable, SimpleTableInfo>>, List<PendingMetaTable>> triple = compareMetaBetweenPdmTables(pendingMetaTables, pdmTables, comparator);
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
                        for (String pk :pdmtable.getPkColumns()) {
                            if (pk.equals(mdColumn.getColumnName())) {
                                mdColumn.setPrimarykey("T");
                                break;
                            } else
                                mdColumn.setPrimarykey("F");
                        }
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
                    Comparator<TableField> columnComparator = (o1, o2) -> StringUtils.compare(o1.getColumnName(), o2.getColumnName());
                    Triple<List<SimpleTableField>, List<Pair<PendingMetaColumn, SimpleTableField>>, List<PendingMetaColumn>> columnCompared = compareMetaBetweenPdmTables(oldColumns, newColumns, columnComparator);
                    if (columnCompared.getLeft() != null && columnCompared.getLeft().size() > 0) {
                        //新增
                        for (SimpleTableField tableField : columnCompared.getLeft()) {
                            PendingMetaColumn metaColumn = new PendingMetaColumn().convertFromTableField(tableField);
                            metaColumn.setTableId(oldTable.getTableId());
                            metaColumn.setRecorder(recorder);
                            for (String pk :newTable.getPkColumns()) {
                                if (pk.equals(metaColumn.getColumnName())) {
                                    metaColumn.setPrimarykey("T");
                                    break;
                                } else
                                    metaColumn.setPrimarykey("F");
                            }
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
                            for (String pk :newTable.getPkColumns()) {
                                if (pk.equals(oldColumn.getColumnName())) {
                                    oldColumn.setPrimarykey("T");
                                    break;
                                } else
                                    oldColumn.setPrimarykey("F");
                            }
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
        List<PendingMetaTable> metaTables = pendingMdTableDao.listObjectsByFilter("where DATABASE_CODE = ?", new Object[]{databaseCode});
        List<Pair<Integer, String>> pairs = new ArrayList<>();
        for (PendingMetaTable pendingMetaTable : metaTables) {
            Pair<Integer, String> pair = publishMetaTable(pendingMetaTable.getTableId(), recorder);
            if (pair.getLeft() != 0) {
                pairs.add(new ImmutablePair<>(0, pair.getRight()));
            }
        }
        if (pairs.size() == 0)
            pairs.add(new ImmutablePair<>(0, "批量发布成功"));
        StringBuffer sPair = new StringBuffer("");
        for (Pair<Integer, String> pair : pairs) {
            sPair.append(pair.getRight())
                .append(";");
        }
        return  new ImmutablePair<>(0, sPair.toString());
    }

    private <K,V> Triple<List<K>, List<Pair<V, K>>, List<V>>
    compareMetaBetweenPdmTables(List<V> metaTables, List<K> simpleTableInfos, Comparator comparator){
        if(metaTables==null ||metaTables.size()==0)
            return new ImmutableTriple<>(
                simpleTableInfos,null,null);
        if(simpleTableInfos==null ||simpleTableInfos.size()==0)
            return new ImmutableTriple<> (
                null,null,metaTables);
        List<V> oldList = CollectionsOpt.cloneList(metaTables);
        List<K> newList = CollectionsOpt.cloneList(simpleTableInfos);
        Collections.sort(oldList, comparator);
        Collections.sort(newList, comparator);
        //---------------------------------------
        int i=0; int sl = oldList.size();
        int j=0; int dl = newList.size();
        List<K> insertList = new ArrayList<>();
        List<V> delList = new ArrayList<>();
        List<Pair<V,K>> updateList = new ArrayList<>();
        while(i<sl&&j<dl){
            int n = comparator.compare(oldList.get(i), newList.get(j));
            if(n<0){
                delList.add(oldList.get(i));
                i++;
            }else if(n==0){
                updateList.add( new ImmutablePair<>(oldList.get(i),newList.get(j)));
                i++;
                j++;
            }else {
                insertList.add(newList.get(j));
                j++;
            }
        }

        while(i<sl){
            delList.add(oldList.get(i));
            i++;
        }

        while(j<dl){
            insertList.add(newList.get(j));
            j++;
        }

        return new ImmutableTriple<>(insertList,updateList,delList);
    }

}

