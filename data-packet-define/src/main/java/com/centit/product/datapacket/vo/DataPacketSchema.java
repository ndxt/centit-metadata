package com.centit.product.datapacket.vo;

import com.centit.product.datapacket.po.DataPacket;
import com.centit.product.datapacket.po.DataPacketParam;
import com.centit.product.datapacket.po.RmdbQuery;
import com.centit.product.datapacket.po.RmdbQueryColumn;
import com.centit.support.algorithm.BooleanBaseOpt;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

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

    @ApiModelProperty(value = "数据查询描述")
    private List<RmdbQuery> rmdbQueries;

    @Data
    class DataSetSchema{
        String dataSetId;
        @ApiModelProperty(value = "数据集名")
        String dataSetName;
        @ApiModelProperty(value = "数据集标题")
        String dataSetTitle;
        @ApiModelProperty(value = "数据集字段列表")
        List<ColumnSchema> columns;
    }

    private List<DataSetSchema> dataSets;

    public DataPacketSchema valueOf(DataPacket dataPacket) {
        if(dataPacket == null){
            return null;
        }
        DataPacketSchema dataPacketSchema = new DataPacketSchema();
        dataPacketSchema.packetId = dataPacket.getPacketId();
        dataPacketSchema.packetName = dataPacket.getPacketName();
        dataPacketSchema.packetType = dataPacket.getPacketType();
        dataPacketSchema.packetDesc = dataPacket.getPacketDesc();
        dataPacketSchema.packetParams = dataPacket.getPacketParams();
        dataPacketSchema.rmdbQueries  = dataPacket.getRmdbQueries();
        List<ColumnSchema> columnSchemas =  new ArrayList<ColumnSchema>();
        List<DataSetSchema> dataSetSchemaList = new ArrayList<DataSetSchema>();
        if(dataPacket.getRmdbQueries()!=null) {
            for (RmdbQuery rdb : dataPacket.getRmdbQueries()) {
                DataSetSchema dataSetSchema = new DataSetSchema();
                dataSetSchema.dataSetId = rdb.getPacketId();
                dataSetSchema.dataSetName = rdb.getQueryName();
                dataSetSchema.dataSetTitle = rdb.getQueryDesc();
                if (rdb.getColumns() != null && rdb.getColumns().size() > 0) {
                    for (RmdbQueryColumn queryColumn : rdb.getColumns()) {
                        ColumnSchema schema = new ColumnSchema();
                        schema.setColumnCode(queryColumn.getColumnCode());
                        schema.setColumnName(queryColumn.getColumnName());
                        schema.setDataType(queryColumn.getDataType());
                        schema.setStatData(BooleanBaseOpt.castObjectToBoolean(queryColumn.getIsStatData(),false));
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
