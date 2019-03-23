package com.centit.product.datapacket.vo;

import com.centit.product.datapacket.po.DataPacket;
import com.centit.product.datapacket.po.DataPacketParam;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.io.Serializable;
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


    private List<DataPacketParam> packetParams;

    @Data
    class ColumnSchema{
        String columnCode;
        String columnName;
        String dataType;
        boolean isStatData;
    }

    @Data
    class DataSetSchema{
        String dataSetId;
        String dataSetName;
        String dataSetTitle;
        List<ColumnSchema> columns;
    }

    private List<DataSetSchema> dataSets;

    public static DataPacketSchema valueOf(DataPacket dataPacket){
        return null;
    }
}
