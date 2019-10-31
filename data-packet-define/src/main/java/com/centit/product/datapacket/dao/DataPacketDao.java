package com.centit.product.datapacket.dao;

import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.datapacket.po.DataPacket;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;

@Repository
public class DataPacketDao extends BaseDaoImpl<DataPacket, String> {
    @Override
    public Map<String, String> getFilterField() {
        if (filterField == null) {
            filterField = new HashMap<>();
            filterField.put("packetName", CodeBook.LIKE_HQL_ID);
            filterField.put("packetId", CodeBook.EQUAL_HQL_ID);
        }
        return filterField;
    }
}
