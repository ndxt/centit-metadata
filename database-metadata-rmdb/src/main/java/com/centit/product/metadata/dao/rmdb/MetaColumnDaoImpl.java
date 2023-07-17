package com.centit.product.metadata.dao.rmdb;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.metadata.dao.MetaColumnDao;
import com.centit.product.metadata.po.MetaColumn;
import org.springframework.stereotype.Repository;

import java.io.Serializable;
import java.util.Map;

@Repository("metaColumnDao")
public class MetaColumnDaoImpl extends BaseDaoImpl<MetaColumn, Serializable> implements MetaColumnDao {
    @Override
    public Map<String, String> getFilterField() {
        return null;
    }

    @Override
    public MetaColumn getObjectById(MetaColumn id) {
        return super.getObjectById(id);
    }
}
