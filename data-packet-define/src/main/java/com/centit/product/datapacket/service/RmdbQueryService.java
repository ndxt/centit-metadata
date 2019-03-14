package com.centit.product.datapacket.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.support.database.utils.PageDesc;
import com.centit.product.datapacket.po.DataPacket;
import com.centit.product.datapacket.po.RmdbQuery;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface RmdbQueryService {

    /**
     * 新增数据包
     */
    void createDataResource(DataPacket dataResource);

    void updateDataResource(DataPacket dataResource);

    void deleteDataResource(String resourceId);

    List<DataPacket> listDataResource(Map<String, Object> params, PageDesc pageDesc);

    DataPacket getDataResource(String resourceId);

    List<RmdbQuery> generateRmdbQuery(String databaseCode, String sql);

    JSONArray queryData(String databaseCode, String sql, Map<String, Object> params);

    Set<String> generateParam(String sql);

}
