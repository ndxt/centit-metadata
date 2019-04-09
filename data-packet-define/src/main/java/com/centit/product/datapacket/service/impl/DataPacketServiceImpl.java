package com.centit.product.datapacket.service.impl;

import com.centit.product.datapacket.dao.DataPacketDao;
import com.centit.product.datapacket.dao.RmdbQueryDao;
import com.centit.product.datapacket.po.DataPacket;
import com.centit.product.datapacket.po.RmdbQuery;
import com.centit.product.datapacket.po.RmdbQueryColumn;
import com.centit.product.datapacket.service.DataPacketService;
import com.centit.support.database.utils.PageDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
@Transactional
public class DataPacketServiceImpl implements DataPacketService {

    private final Logger logger = LoggerFactory.getLogger(DataPacketServiceImpl.class);

    @Autowired
    private DataPacketDao dataPacketDao;

    @Autowired
    private RmdbQueryDao rmdbQueryDao;

    @Override
    public void createDataPacket(DataPacket dataPacket) {
        dataPacketDao.saveNewObject(dataPacket);
        dataPacketDao.saveObjectReferences(dataPacket);
        if (dataPacket.getRmdbQueries()!=null && dataPacket.getRmdbQueries().size() > 0) {
            for (RmdbQuery db : dataPacket.getRmdbQueries()) {
                for (RmdbQueryColumn column : db.getColumns()) {
                    column.setPacketId(db.getPacketId());
                }
                rmdbQueryDao.saveObjectReferences(db);
            }
        }
    }

    @Override
    public void updateDataPacket(DataPacket dataPacket) {
        dataPacketDao.updateObject(dataPacket);
        dataPacketDao.saveObjectReferences(dataPacket);
        if (dataPacket.getRmdbQueries()!=null && dataPacket.getRmdbQueries().size() > 0) {
            for (RmdbQuery db : dataPacket.getRmdbQueries()) {
                for (RmdbQueryColumn column : db.getColumns()) {
                    column.setPacketId(db.getPacketId());
                }
                rmdbQueryDao.saveObjectReferences(db);
            }
        }
    }

    @Override
    public void deleteDataPacket(String packetId) {
        DataPacket dataPacket = dataPacketDao.getObjectWithReferences(packetId);
        if (dataPacket!=null && dataPacket.getRmdbQueries()!=null && dataPacket.getRmdbQueries().size() > 0) {
            for (RmdbQuery db : dataPacket.getRmdbQueries()) {
                rmdbQueryDao.deleteObjectReferences(db);
            }
        }
        dataPacketDao.deleteObjectById(packetId);
        dataPacketDao.deleteObjectReferences(dataPacket);
    }

    @Override
    public List<DataPacket> listDataPacket(Map<String, Object> params, PageDesc pageDesc) {
        return dataPacketDao.listObjectsByProperties(params, pageDesc);
    }

    @Override
    public DataPacket getDataPacket(String packetId) {
        DataPacket dataPacket = dataPacketDao.getObjectWithReferences(packetId);
        if (dataPacket!=null && dataPacket.getRmdbQueries()!=null && dataPacket.getRmdbQueries().size() >0 ) {
            for (RmdbQuery db : dataPacket.getRmdbQueries()) {
                rmdbQueryDao.fetchObjectReferences(db);
            }
        }
        return dataPacket;
    }

}
