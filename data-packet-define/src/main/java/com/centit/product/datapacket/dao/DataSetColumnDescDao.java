package com.centit.product.datapacket.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.datapacket.po.DataSetColumnDesc;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;

@Repository
public class DataSetColumnDescDao extends BaseDaoImpl<DataSetColumnDesc, HashMap<String,Object>> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }
}
