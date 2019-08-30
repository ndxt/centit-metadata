package com.centit.product.datapacket.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.datapacket.po.DataSetDefine;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public class DataSetDefineDao extends BaseDaoImpl<DataSetDefine, String> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }
}
