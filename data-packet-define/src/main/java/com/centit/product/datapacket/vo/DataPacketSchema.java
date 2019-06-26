package com.centit.product.datapacket.vo;

import com.centit.product.datapacket.po.DataPacket;
import com.centit.product.datapacket.po.DataPacketParam;
import com.centit.product.datapacket.po.RmdbQuery;
import com.centit.product.datapacket.po.RmdbQueryColumn;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class DataPacketSchema implements Serializable {
    private static final long serialVersionUID = 1;

    @ApiModelProperty(value = "数据包ID", hidden = true)
    private String packetId;

    @ApiModelProperty(value = "数据包名称模板")
    private String packetName;
    /**
     * 数据包类别，主要有 D database， F file ， P directory 文件夹 , 默认值为D
     */
    @ApiModelProperty(value = "数据包类别，主要有 D database, F file, P directory 文件夹, 默认值为D")
    private String packetType;

    /**
     * 详细描述
     */
    @ApiModelProperty(value = "详细描述")
    private String packetDesc;

    @ApiModelProperty(value = "数据包参数列表")
    private List<DataPacketParam> packetParams;

    @ApiModelProperty(value = "数据集描述")
    private List<DataSetSchema> dataSets;

    public void putDataSetSchema(DataSetSchema dss){
        if(dataSets==null){
            dataSets = new ArrayList<>();
            dataSets.add(dss);
            return;
        }
        int n=dataSets.size();
        for(int i=0; i<n; i++){
            if(StringUtils.equals(dataSets.get(i).getDataSetName(), dss.getDataSetName())){
                dataSets.set(i,dss);
                return;
            }
        }
        dataSets.add(dss);
    }

    public DataSetSchema fetchDataSetSchema(String dataSetName){
        if(dataSets==null){
            return null;
        }
        for(DataSetSchema dss : dataSets){
            if(StringUtils.equals(dss.getDataSetName(), dataSetName)){
                return dss;
            }
        }
        return null;
    }

    public static DataPacketSchema valueOf(DataPacket dataPacket) {
        if(dataPacket == null){
            return null;
        }
        DataPacketSchema dataPacketSchema = new DataPacketSchema();
        dataPacketSchema.packetId = dataPacket.getPacketId();
        dataPacketSchema.packetName = dataPacket.getPacketName();
        dataPacketSchema.packetType = dataPacket.getPacketType();
        dataPacketSchema.packetDesc = dataPacket.getPacketDesc();
        dataPacketSchema.packetParams = dataPacket.getPacketParams();

        List<DataSetSchema> dataSetSchemaList = new ArrayList<>();
        if(dataPacket.getRmdbQueries()!=null) {
            for (RmdbQuery rdb : dataPacket.getRmdbQueries()) {
                DataSetSchema dataSetSchema = new DataSetSchema();
                List<ColumnSchema> columnSchemas =  new ArrayList<>();
                dataSetSchema.dataSetId = rdb.getQueryId();
                dataSetSchema.dataSetName = rdb.getQueryName();
                dataSetSchema.dataSetTitle = rdb.getQueryDesc();
                if (rdb.getColumns() != null && rdb.getColumns().size() > 0) {
                    for (RmdbQueryColumn queryColumn : rdb.getColumns()) {
                        ColumnSchema schema = new ColumnSchema();
                        schema.setColumnCode(queryColumn.getColumnCode());
                        schema.setPropertyName(queryColumn.getPropertyName());
                        schema.setColumnName(queryColumn.getColumnName());
                        schema.setDataType(queryColumn.getDataType());
                        schema.setIsStatData(queryColumn.getIsStatData());
                        columnSchemas.add(schema);
                    }
                }
                dataSetSchema.setColumns(columnSchemas);
                dataSetSchemaList.add(dataSetSchema);
            }
        }
        dataPacketSchema.setDataSets(dataSetSchemaList);
        return dataPacketSchema;
    }
}
