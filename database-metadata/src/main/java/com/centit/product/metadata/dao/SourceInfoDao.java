package com.centit.product.metadata.dao;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.framework.jdbc.dao.JdbcTemplateUtils;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.product.metadata.transaction.AbstractDruidConnectPools;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.database.utils.PageDesc;
import com.centit.support.database.utils.QueryUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class SourceInfoDao extends BaseDaoImpl<SourceInfo, String> {

    @Override
    public Map<String, String> getFilterField() {
        Map<String, String> filterField = new HashMap<>();
        filterField.put("databaseName", CodeBook.LIKE_HQL_ID);
        filterField.put("databaseCode", CodeBook.EQUAL_HQL_ID);
        filterField.put("osId", CodeBook.EQUAL_HQL_ID);
        filterField.put("databaseType", CodeBook.LIKE_HQL_ID);
        filterField.put("hostPort", CodeBook.LIKE_HQL_ID);
        filterField.put("databaseUrl", CodeBook.LIKE_HQL_ID);
        filterField.put("username", CodeBook.LIKE_HQL_ID);
        filterField.put("password", CodeBook.LIKE_HQL_ID);
        filterField.put("dataDesc", CodeBook.LIKE_HQL_ID);
        filterField.put("createTime", CodeBook.LIKE_HQL_ID);
        filterField.put("created", CodeBook.LIKE_HQL_ID);
        filterField.put("sourceType", CodeBook.EQUAL_HQL_ID);
        filterField.put("topUnit",CodeBook.EQUAL_HQL_ID);
        return filterField;
    }


    public List<SourceInfo> listDatabase() {
        return this.listObjects();
    }

    public SourceInfo getDatabaseInfoById(String databaseCode) {
        return this.getObjectById(databaseCode);
    }

    //jdbc
    public String getNextKey() {
        return StringBaseOpt.fillZeroForString(
            String.valueOf(
                JdbcTemplateUtils.getSequenceNextValue(
                    this.jdbcTemplate, "S_DATABASECODE")), 10);
    }

    public JSONArray queryDatabaseAsJson(String databaseName, PageDesc pageDesc) {
        if (StringUtils.isBlank(databaseName)) {
            return super.listObjectsAsJson(new HashMap<>(1), pageDesc);
        }
        String matchStr = QueryUtils.getMatchString(databaseName);
        return super.listObjectsByFilterAsJson("where DATABASE_NAME like ? or DATABASE_URL like ?", new Object[]{matchStr, matchStr}, pageDesc);
    }
}
