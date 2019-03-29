package com.centit.product.datapacket.vo;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class DataSetSchema{
    public DataSetSchema(String dataSetName){
        this.dataSetId = dataSetName;
        this.dataSetName = dataSetName;
        this.dataSetTitle = dataSetName;
    }
    @ApiModelProperty(value = "数据ID，一般和数据集名一样")
    String dataSetId;
    @ApiModelProperty(value = "数据集名")
    String dataSetName;
    @ApiModelProperty(value = "数据集标题")
    String dataSetTitle;
    @ApiModelProperty(value = "数据集字段列表")
    List<ColumnSchema> columns;
}
