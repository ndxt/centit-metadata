package com.centit.support.metadata.dao;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.support.metadata.po.MetaRelation;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public class MetaRelationDao extends BaseDaoImpl<MetaRelation, Long> {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }
}
