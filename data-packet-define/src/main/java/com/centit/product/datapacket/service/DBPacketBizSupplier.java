package com.centit.product.datapacket.service;

import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.product.dataopt.core.*;
import com.centit.product.dataopt.dataset.ExcelDataSet;
import com.centit.product.dataopt.dataset.FileDataSet;
import com.centit.product.dataopt.dataset.SQLDataSetReader;
import com.centit.product.datapacket.po.DataPacket;
import com.centit.product.datapacket.po.DataSetDefine;
import com.centit.support.database.utils.DataSourceDescription;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class DBPacketBizSupplier implements BizSupplier {

    private DataPacket dbPacket;
    private IntegrationEnvironment integrationEnvironment;
    private Map<String, Object> queryParams;

    public DBPacketBizSupplier(DataPacket dbPacket){
        this.dbPacket = dbPacket;
    }

    /**
     * Gets a result.
     *
     * @return a result
     */
    @Override
    public BizModel get() {
        SimpleBizModel bizModel = new SimpleBizModel(this.dbPacket.getPacketName());
        Map<String, DataSet> dataSets = new HashMap<>(this.dbPacket.getDataSetDefines()!=null ?
            this.dbPacket.getDataSetDefines().size()+1 : 1);
        Map<String, Object> modelTag = this.dbPacket.getPacketParamsValue();
        if(queryParams!=null && queryParams.size()>0) {
            modelTag.putAll(queryParams);
        }
        if (this.dbPacket.getDataSetDefines() !=null && this.dbPacket.getDataSetDefines().size() >0) {
            for (DataSetDefine rdd : this.dbPacket.getDataSetDefines()) {
                if("D".equals(rdd.getSetType())) {
                    SQLDataSetReader sqlDSR = new SQLDataSetReader();
                    sqlDSR.setDataSource(DataSourceDescription.valueOf(
                        integrationEnvironment.getDatabaseInfo(rdd.getDatabaseCode())));
                    sqlDSR.setSqlSen(rdd.getQuerySQL());
                    SimpleDataSet dataset = sqlDSR.load(modelTag);
                    dataset.setDataSetName(rdd.getQueryName());
                    dataSets.put(rdd.getQueryId(), dataset);
                } else if ("E".equals(rdd.getSetType())){
                    ExcelDataSet excelDataSet = new ExcelDataSet();
                    try {
                        excelDataSet.setFilePath(FileDataSet.downFile(rdd.getQuerySQL()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    SimpleDataSet dataset = excelDataSet.load(modelTag);
                    dataset.setDataSetName(rdd.getQueryName());
                    dataSets.put(rdd.getQueryId(), dataset);
                }
            }
        }
        bizModel.setModelTag(modelTag);
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
