package com.centit.product.metadata.dao;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.framework.jdbc.dao.DatabaseOptUtils;
import com.centit.product.metadata.po.MetaTable;
import com.centit.support.algorithm.CollectionsOpt;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;

@Repository
public class MetaTableDao extends BaseDaoImpl<MetaTable, String> {
    @Override
    public Map<String, String> getFilterField() {
        Map<String, String> filterField = new HashMap<>();
        filterField.put("accessType", CodeBook.IN_HQL_ID);
        filterField.put("tableType", CodeBook.IN_HQL_ID);
        filterField.put("databaseCode", CodeBook.EQUAL_HQL_ID);
        filterField.put("tableId", CodeBook.EQUAL_HQL_ID);
        filterField.put("tableName", CodeBook.LIKE_HQL_ID);
        filterField.put("tableLabelName", CodeBook.LIKE_HQL_ID);
        return  filterField;
    }

    public MetaTable getMetaTable(String databaseCode, String tableName) {
        return super.getObjectByProperties(
            CollectionsOpt.createHashMap("databaseCode",databaseCode,"tableName",tableName));
    }


    /**
     * 根据osId过滤MetaTable数据
     * @param parameters parameters
     * @return
     */
    public JSONArray getMetaTableList(Map<String, Object> parameters) {
        String sql = " SELECT A.DATABASE_NAME , B.TABLE_ID, B.TABLE_LABEL_NAME, B.DATABASE_CODE, B.TABLE_NAME, B.TABLE_TYPE, B.ACCESS_TYPE, B.TABLE_COMMENT, B.WORKFLOW_OPT_TYPE AS WORK_FLOW_OPT_TYPE, B.RECORD_DATE,\n" +
            " B.RECORDER, B.UPDATE_CHECK_TIMESTAMP, B.FULLTEXT_SEARCH, B.WRITE_OPT_LOG, B.OBJECT_TITLE   FROM F_DATABASE_INFO A JOIN F_MD_TABLE B ON A.DATABASE_CODE = B.DATABASE_CODE " +
            "  WHERE  1 = 1 [ :topUnit | AND A.TOP_UNIT = :topUnit ]  [ :databaseCode | AND A.DATABASE_CODE = :databaseCode ] " +
            " [ :(like)tableName | AND B.TABLE_NAME LIKE :tableName ]  [ :(like)tableLabelName | AND B.TABLE_NAME LIKE :tableLabelName ] ";
        return DatabaseOptUtils.listObjectsByParamsDriverSqlAsJson(this,sql,parameters);
    }

    /**
     * 根据optId过滤MetaTable数据
     * @param optId optId
     * @return
     */
    public JSONArray getMetaTableListByOptId(String optId) {
        String sql = "SELECT A.ID, B.TABLE_ID, B.TABLE_LABEL_NAME, B.DATABASE_CODE, B.TABLE_NAME, B.TABLE_TYPE, B.ACCESS_TYPE, B.TABLE_COMMENT, B.WORKFLOW_OPT_TYPE AS WORK_FLOW_OPT_TYPE, B.RECORD_DATE,\n" +
            " B.RECORDER, B.UPDATE_CHECK_TIMESTAMP, B.FULLTEXT_SEARCH, B.WRITE_OPT_LOG, B.OBJECT_TITLE ,C.DATABASE_NAME FROM  F_TABLE_OPT_RELATION A JOIN f_md_table B ON  A.TABLE_ID = B.TABLE_ID " +
            "  JOIN  f_database_info C ON B.DATABASE_CODE =C.DATABASE_CODE  WHERE A.OPT_ID = ? ";
        return DatabaseOptUtils.listObjectsBySqlAsJson(this, sql, new Object[]{optId});
    }
}
