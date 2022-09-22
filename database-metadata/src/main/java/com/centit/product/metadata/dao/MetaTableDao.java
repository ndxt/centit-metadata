package com.centit.product.metadata.dao;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.framework.jdbc.dao.DatabaseOptUtils;
import com.centit.product.adapter.po.MetaTable;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.NumberBaseOpt;
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
        filterField.put("(splitforin)tableNames","upper(table_name) in (:tableNames)");
        filterField.put("(splitforin)databaseCode_in","database_code in (:databaseCode_in)");
        filterField.put("(like)likeTableNameOrLabel", " ( TABLE_NAME LIKE :likeTableNameOrLabel OR TABLE_LABEL_NAME LIKE :likeTableNameOrLabel  ) ");
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
        String sql = " SELECT A.DATABASE_NAME, A.SOURCE_TYPE,  B.TABLE_ID, B.TABLE_LABEL_NAME, B.DATABASE_CODE, B.TABLE_NAME, B.TABLE_TYPE, B.ACCESS_TYPE, B.TABLE_COMMENT, B.WORKFLOW_OPT_TYPE AS WORK_FLOW_OPT_TYPE, B.RECORD_DATE,\n" +
            " B.RECORDER, B.UPDATE_CHECK_TIMESTAMP, B.FULLTEXT_SEARCH, B.WRITE_OPT_LOG, B.OBJECT_TITLE   FROM F_DATABASE_INFO A JOIN F_MD_TABLE B ON A.DATABASE_CODE = B.DATABASE_CODE " +
            "  WHERE  1 = 1 [ :topUnit | AND A.TOP_UNIT = :topUnit ]  [ :databaseCode | AND A.DATABASE_CODE = :databaseCode ] " +
            " [ :(like)tableName | AND B.TABLE_NAME LIKE :tableName ]  [ :(like)tableLabelName | AND B.TABLE_LABEL_NAME LIKE :tableLabelName ]  [ :sourceType | AND A.SOURCE_TYPE = :sourceType ] " +
            " [ :(like)likeTableNameOrLabel | AND ( B.TABLE_NAME LIKE :likeTableNameOrLabel OR B.TABLE_LABEL_NAME LIKE :likeTableNameOrLabel  ) ]  ";
        return DatabaseOptUtils.listObjectsByParamsDriverSqlAsJson(this,sql,parameters);
    }

    /**
     * 根据optId过滤MetaTable数据
     * @param parameters parameters
     * @return
     */
    public JSONArray getMetaTableListWithTableOptRelation(Map<String, Object> parameters) {
        String sql = "SELECT A.ID, B.TABLE_ID, B.TABLE_LABEL_NAME, B.DATABASE_CODE, B.TABLE_NAME, B.TABLE_TYPE, B.ACCESS_TYPE, B.TABLE_COMMENT, B.WORKFLOW_OPT_TYPE AS WORK_FLOW_OPT_TYPE, B.RECORD_DATE,\n" +
            " B.RECORDER, B.UPDATE_CHECK_TIMESTAMP, B.FULLTEXT_SEARCH, B.WRITE_OPT_LOG, B.OBJECT_TITLE ,C.DATABASE_NAME, C.SOURCE_TYPE  FROM  F_TABLE_OPT_RELATION A JOIN F_MD_TABLE B ON  A.TABLE_ID = B.TABLE_ID " +
            "  JOIN  F_DATABASE_INFO C ON B.DATABASE_CODE =C.DATABASE_CODE  WHERE  1 = 1  [ :optId | AND A.OPT_ID = :optId  ] [ :osId | AND A.OS_ID = :osId  ]  [ :sourceType | AND C.SOURCE_TYPE = :sourceType ] " +
            " [ :(like)tableName | AND B.TABLE_NAME LIKE :tableName ]  [ :(like)tableLabelName | AND B.TABLE_NAME LIKE :tableLabelName ] [ :(like)likeTableNameOrLabel | AND ( B.TABLE_NAME LIKE :likeTableNameOrLabel OR B.TABLE_LABEL_NAME LIKE :likeTableNameOrLabel  ) ] ";
        return DatabaseOptUtils.listObjectsByParamsDriverSqlAsJson(this,sql,parameters);
    }

    public boolean isTableExist(String tableName, String dataBaseCode) {
        String sql = " SELECT COUNT(1) FROM F_MD_TABLE WHERE TABLE_NAME = ? AND DATABASE_CODE = ?  ";
        return NumberBaseOpt.castObjectToInteger(DatabaseOptUtils.getScalarObjectQuery(this, sql, new Object[]{tableName, dataBaseCode})) >0;
    }
}
