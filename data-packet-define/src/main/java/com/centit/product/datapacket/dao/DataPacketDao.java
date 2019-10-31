package com.centit.product.datapacket.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.datapacket.po.DataPacket;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public class DataPacketDao extends BaseDaoImpl<DataPacket, String> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }
}
