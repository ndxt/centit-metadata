package com.centit.product.metadata.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.metadata.po.MetaRelation;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public class MetaRelationDao extends BaseDaoImpl<MetaRelation, Long> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }

}
