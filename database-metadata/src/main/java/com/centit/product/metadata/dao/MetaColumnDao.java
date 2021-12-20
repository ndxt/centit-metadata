package com.centit.product.metadata.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.adapter.po.MetaColumn;
import org.springframework.stereotype.Repository;

import java.io.Serializable;
import java.util.Map;

@Repository
public class MetaColumnDao extends BaseDaoImpl<MetaColumn, Serializable> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }
}
