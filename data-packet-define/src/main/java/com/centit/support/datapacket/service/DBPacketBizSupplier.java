package com.centit.support.datapacket.service;

import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.support.database.utils.DataSourceDescription;
import com.centit.support.dataopt.core.BizModel;
import com.centit.support.dataopt.core.BizSupplier;
import com.centit.support.dataopt.core.DataSet;
import com.centit.support.dataopt.core.SimpleBizModel;
import com.centit.support.dataopt.dataset.SQLDataSetReader;
import com.centit.support.datapacket.po.DataPacket;
import com.centit.support.datapacket.po.RmdbQuery;

import java.util.HashMap;
import java.util.Map;


public class DBPacketBizSupplier implements BizSupplier {

    private DataPacket dbPacket;
    private IntegrationEnvironment integrationEnvironment;
    private Map<String, Object> queryParams;

    public DBPacketBizSupplier(DataPacket dbPacket){
        this.dbPacket = dbPacket;
    }

    public static DataSourceDescription mapDataSource(DatabaseInfo dbinfo) {
        DataSourceDescription desc=new DataSourceDescription();
        desc.setConnUrl(dbinfo.getDatabaseUrl());
        desc.setUsername(dbinfo.getUsername());
        desc.setPassword(dbinfo.getClearPassword());
        return desc;
    }

    /**
     * Gets a result.
     *
     * @return a result
     */
    @Override
    public BizModel get() {
        SimpleBizModel bizModel = new SimpleBizModel(this.dbPacket.getPacketName());
        Map<String, DataSet> dataSets = new HashMap<>(this.dbPacket.getDBQueries().size()+1);
        for(RmdbQuery rdd : this.dbPacket.getDBQueries()) {
            SQLDataSetReader sqlDSR = new SQLDataSetReader();
            sqlDSR.setDataSource( mapDataSource(
                integrationEnvironment.getDatabaseInfo(rdd.getDatabaseCode())));
            sqlDSR.setSqlSen(rdd.getQuerySQL());
            dataSets.put(rdd.getQueryName(), sqlDSR.load(this.queryParams));
        }
        bizModel.setModeTag(queryParams);
        bizModel.setBizData(dataSets);
        return bizModel;
    }

    public void setDbPacket(DataPacket dbPacket) {
        this.dbPacket = dbPacket;
    }

    public void setIntegrationEnvironment(IntegrationEnvironment integrationEnvironment) {
        this.integrationEnvironment = integrationEnvironment;
    }

    public void setQueryParams(Map<String, Object> queryParams) {
        this.queryParams = queryParams;
    }

    /**
     * 业务数据是否是 批量的
     * 如果是，处理器将反复调用 。知道 get() 返回 null 结束
     *
     * @return 否是 批量的
     */
    @Override
    public boolean isBatchWise() {
        return false;
    }

}
