package com.centit.product.datapacket.service;

import com.alibaba.fastjson.JSONArray;
import com.centit.support.database.utils.PageDesc;
import com.centit.product.datapacket.po.DataPacket;
import com.centit.product.datapacket.po.RmdbQuery;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface DataPacketService {

    /**
     * 新增数据包
     */
    void createDataPacket(DataPacket dataPacket);

    void updateDataPacket(DataPacket dataPacket);

    void deleteDataPacket(String packetId);

    List<DataPacket> listDataPacket(Map<String, Object> params, PageDesc pageDesc);

    DataPacket getDataPacket(String packetId);

    List<RmdbQuery> generateRmdbQuery(String databaseCode, String sql);

    JSONArray queryData(String databaseCode, String sql, Map<String, Object> params);

    Set<String> generateParam(String sql);

}
