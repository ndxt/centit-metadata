package com.centit.product.datapacket.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.product.datapacket.vo.ColumnSchema;
import com.centit.support.database.utils.PageDesc;
import com.centit.product.datapacket.po.DataPacket;
import com.centit.product.datapacket.po.RmdbQuery;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RmdbQueryService {

    /**
     * 新增数据库查询
     */
    void createDbQuery(RmdbQuery rmdbQuery);

    void updateDbQuery(RmdbQuery rmdbQuery);

    void deleteDbQuery(String queryId);

    List<RmdbQuery> listDbQuery(Map<String, Object> params, PageDesc pageDesc);

    RmdbQuery getDbQuery(String queryId);

    List<ColumnSchema> generateSqlFields(String databaseCode, String sql, Map<String, Object> params);

    JSONArray queryViewSqlData(String databaseCode, String sql, Map<String, Object> params);

    Set<String> generateSqlParams(String sql);

}
