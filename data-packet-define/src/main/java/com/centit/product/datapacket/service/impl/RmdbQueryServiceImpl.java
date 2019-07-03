package com.centit.product.datapacket.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.common.ObjectException;
import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.product.datapacket.vo.ColumnSchema;
import com.centit.support.database.jsonmaptable.GeneralJsonObjectDao;
import com.centit.support.database.utils.*;
import com.centit.product.datapacket.dao.DataPacketDao;
import com.centit.product.datapacket.dao.RmdbQueryDao;
import com.centit.product.datapacket.po.DataPacket;
import com.centit.product.datapacket.po.RmdbQuery;
import com.centit.product.datapacket.service.RmdbQueryService;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@Transactional
public class RmdbQueryServiceImpl implements RmdbQueryService {

    private final Logger logger = LoggerFactory.getLogger(RmdbQueryServiceImpl.class);

    @Autowired
    private DataPacketDao dataPacketDao;

    @Autowired
    private RmdbQueryDao resourceColumnDao;

    @Autowired
    private IntegrationEnvironment integrationEnvironment;

    @Override
    public void createDbQuery(RmdbQuery rmdbQuery) {
        resourceColumnDao.saveNewObject(rmdbQuery);
        resourceColumnDao.saveObjectReferences(rmdbQuery);
    }

    @Override
    public void updateDbQuery(RmdbQuery rmdbQuery) {
        rmdbQuery.setRecordDate(new Date());
        resourceColumnDao.updateObject(rmdbQuery);
        resourceColumnDao.saveObjectReferences(rmdbQuery);
    }

    @Override
    public void deleteDbQuery(String queryId) {
        RmdbQuery rmdbQuery = resourceColumnDao.getObjectById(queryId);
        resourceColumnDao.deleteObjectById(queryId);
        resourceColumnDao.deleteObjectReferences(rmdbQuery);
    }

    @Override
    public List<RmdbQuery> listDbQuery(Map<String, Object> params, PageDesc pageDesc) {
        return resourceColumnDao.listObjectsByProperties(params, pageDesc);
    }

    @Override
    public RmdbQuery getDbQuery(String queryId) {
        return resourceColumnDao.getObjectWithReferences(queryId);
    }

    @Override
    public JSONArray queryViewSqlData(String databaseCode, String sql, Map<String, Object> params) {
        DatabaseInfo databaseInfo = integrationEnvironment.getDatabaseInfo(databaseCode);
        QueryAndParams qap = QueryAndParams.createFromQueryAndNamedParams(QueryUtils.translateQuery(sql, params));
        try{
            return TransactionHandler.executeQueryInTransaction(JdbcConnect.mapDataSource(databaseInfo),
                (conn) -> DatabaseAccess.findObjectsAsJSON(conn,
                QueryUtils.buildLimitQuerySQL(qap.getQuery(),0,20,false,
                    DBType.mapDBType(databaseInfo.getDatabaseUrl())),
                qap.getParams()));
        }catch (SQLException | IOException e){
            logger.error("执行查询出错，SQL：{},Param:{}", qap.getQuery(), qap.getParams());
            throw new ObjectException("执行查询出错!");
        }
    }

    @Override
    public Set<String> generateSqlParams(String sql) {
        return QueryUtils.getSqlTemplateParameters(sql);
    }

    @Override
    public List<ColumnSchema> generateSqlFields(String databaseCode, String sql, Map<String, Object> params){
        DatabaseInfo databaseInfo = integrationEnvironment.getDatabaseInfo(databaseCode);
        QueryAndParams qap = QueryAndParams.createFromQueryAndNamedParams(QueryUtils.translateQuery(sql, params));
        String sSql = QueryUtils.buildLimitQuerySQL(qap.getQuery(),0,2,false,
            DBType.mapDBType(databaseInfo.getDatabaseUrl()));
        List<ColumnSchema> columnSchemas = new ArrayList<>(50);
        try(Connection conn = JdbcConnect.getConn(databaseInfo);
            PreparedStatement stmt = conn.prepareStatement(sSql)){

            DatabaseAccess.setQueryStmtParameters(stmt,qap.getParams());
            try(ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData rsd = rs.getMetaData();
                int nc =rsd.getColumnCount();
                for(int i=1; i<=nc; i++){
                    ColumnSchema col = new ColumnSchema();
                    col.setColumnCode(rsd.getColumnName(i));
                    col.setPropertyName(FieldType.mapPropName(rsd.getColumnName(i)));
                    col.setColumnName(col.getPropertyName());
                    col.setDataType(FieldType.mapToJavaType(rsd.getColumnType(i)));
                    col.setIsStatData("F");
                    columnSchemas.add(col);
                }
            }
        }catch (SQLException e){
            logger.error("执行查询出错，SQL：{},Param:{}", sSql, qap.getParams());
            //throw new ObjectException("执行查询出错!");
            List<String> fields = QueryUtils.getSqlFiledNames(sql);
            if(fields==null){
                throw new ObjectException(sSql, ObjectException.DATABASE_OPERATE_EXCEPTION,
                    "执行查询出错，SQL：{},Param:{}"+ sSql);
            }
            for(String s : QueryUtils.getSqlFiledNames(sql)){
                ColumnSchema col = new ColumnSchema();
                col.setColumnCode(s);
                col.setColumnName(s);
                col.setDataType(FieldType.STRING);
                col.setIsStatData("F");
                columnSchemas.add(col);
            }
        }
        return columnSchemas;
    }
}
