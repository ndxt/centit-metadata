package com.centit.support.datapacket.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.common.ObjectException;
import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.support.database.utils.*;
import com.centit.support.datapacket.dao.DataPacketDao;
import com.centit.support.datapacket.dao.RmdbQueryDao;
import com.centit.support.datapacket.po.DataPacket;
import com.centit.support.datapacket.po.RmdbQuery;
import com.centit.support.datapacket.service.DataPacketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@Transactional
public class DataPacketServiceImpl implements DataPacketService {

    private final Logger logger = LoggerFactory.getLogger(DataPacketServiceImpl.class);

    @Autowired
    private DataPacketDao dataPacketDao;

    @Autowired
    private RmdbQueryDao resourceColumnDao;

    @Autowired
    private IntegrationEnvironment integrationEnvironment;

    @Override
    public void createDataResource(DataPacket dataResource) {
        dataPacketDao.saveNewObject(dataResource);
        dataPacketDao.saveObjectReferences(dataResource);
    }

    @Override
    public void updateDataResource(DataPacket dataResource) {
        dataPacketDao.updateObject(dataResource);
        dataPacketDao.saveObjectReferences(dataResource);
    }

    @Override
    public void deleteDataResource(String resourceId) {
        DataPacket dataResource = dataPacketDao.getObjectById(resourceId);
        dataPacketDao.deleteObjectById(resourceId);
        dataPacketDao.deleteObjectReferences(dataResource);
    }

    @Override
    public List<DataPacket> listDataResource(Map<String, Object> params, PageDesc pageDesc) {
        return dataPacketDao.listObjectsByProperties(params, pageDesc);
    }

    @Override
    public DataPacket getDataResource(String resourceId) {
        return dataPacketDao.getObjectWithReferences(resourceId);
    }

    @Override
    public List<RmdbQuery> generateRmdbQuery(String databaseCode, String sql) {
        return null;
    }


    @Override
    public JSONArray queryData(String databaseCode, String sql, Map<String, Object> params) {
        DatabaseInfo databaseInfo = integrationEnvironment.getDatabaseInfo(databaseCode);
        QueryAndParams qap = QueryAndParams.createFromQueryAndNamedParams(new QueryAndNamedParams(sql, params));

        try (Connection connection = DbcpConnectPools.getDbcpConnect(
            new DataSourceDescription(databaseInfo.getDatabaseUrl(), databaseInfo.getUsername(), databaseInfo.getClearPassword()))){

            return DatabaseAccess.findObjectsAsJSON(connection, qap.getQuery(), qap.getParams());

        }catch (SQLException | IOException e){
            logger.error("执行查询出错，SQL：{},Param:{}", qap.getQuery(), qap.getParams());
            throw new ObjectException("执行查询出错!");
        }
    }

    @Override
    public Set<String> generateParam(String sql) {
        return QueryUtils.getSqlTemplateParameters(sql);
    }


}
