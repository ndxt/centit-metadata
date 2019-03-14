package com.centit.product.datapacket.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.datapacket.po.DataPacketParam;

import java.util.HashMap;
import java.util.Map;

/**
 *  HashMap String,Object 为符合主键，
 */
public class DataPacketParamDao extends BaseDaoImpl<DataPacketParam, HashMap<String,Object>> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }
}
