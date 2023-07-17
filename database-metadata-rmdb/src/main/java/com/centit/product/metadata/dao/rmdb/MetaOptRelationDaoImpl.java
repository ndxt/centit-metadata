package com.centit.product.metadata.dao.rmdb;

import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.metadata.dao.MetaOptRelationDao;
import com.centit.product.metadata.po.MetaOptRelation;
import org.springframework.stereotype.Repository;

@Repository
public class MetaOptRelationDaoImpl extends BaseDaoImpl<MetaOptRelation,String> implements MetaOptRelationDao {
}
