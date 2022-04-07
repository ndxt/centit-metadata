package com.centit.product.dbdesign.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.jdbc.service.BaseEntityManager;
import com.centit.product.adapter.po.*;
import com.centit.support.database.utils.PageDesc;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

/**
 * MdTable  Service.
 * create by scaffold 2016-06-02
 * <p>
 * 表元数据表状态分为 系统/查询/更新
 * 系统，不可以做任何操作
 * 查询，仅用于通用查询模块，不可以更新
 * 更新，可以更新
 */

public interface MetaTableManager extends BaseEntityManager<MetaTable, String> {
    JSONArray listMdTablesAsJson(
        String[] fields,
        Map<String, Object> filterMap, PageDesc pageDesc);

    void saveNewPendingMetaTable(PendingMetaTable pmt);

    void deletePendingMetaTable(String tableId);

    PendingMetaTable getPendingMetaTable(String tableId);

    MetaChangLog getMetaChangLog(String changeId);

    void savePendingMetaTable(PendingMetaTable pmt);

    List<String> makeAlterTableSqlList(String tableId);
    List<String> makeAlterTableSqlList(PendingMetaTable ptable);
    Pair<Integer, String> publishMetaTable(String tableId, String currentUser);

    JSONArray listDrafts(String[] fields, Map<String, Object> searchColumn, PageDesc pageDesc);

    List<Pair<String, String>> listTablesInPdm(String pdmFilePath);

    boolean importTableFromPdm(String pdmFilePath, String tableCode, String databaseCode);

    List<MetaColumn> getNotInFormFields(String tableId);

    List<PendingMetaColumn> listMetaColumns(String tableId, PageDesc pageDesc);

    PendingMetaColumn getMetaColumn(String tableId, String columnName);

    List<MetaColumn> listFields(String tableId);

    Pair<Integer, String>  syncPdm(String databaseCode, String pdmFilePath, List<String> tables, String recorder);

    Pair<Integer, String>  publishDatabase(String databaseCode,String recorder);

    void updateMetaTable(PendingMetaTable metaTable);

    void updateMetaColumn(PendingMetaColumn metaColumn);

    MetaTable getMetaTableWithReferences(String tableId);

    List listCombineTablesByProperty(Map<String, Object> parameters, PageDesc pageDesc);


    /**
     * 检查 F_MD_TABLE 或 F_PENDING_META_TABLE 是否存在 tableName 表
     * @param tableName 表名
     * @param dataBaseCode 数据库code
     * @return
     */
    boolean isTableExist(String tableName,String dataBaseCode);

    void syncDb(String databaseCode, String userCode, String tableName);

    /**
     * 初始化pending数据
     * @param tableId 表id
     * @param userCode 操作人userCode
     * @return
     */
    PendingMetaTable initPendingMetaTable(String tableId, String userCode);
}
