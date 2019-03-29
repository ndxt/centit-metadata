package com.centit.product.datapacket.vo;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;

@Data
public class DataSetSchema{
    String dataSetId;
    @ApiModelProperty(value = "数据集名")
    String dataSetName;
    @ApiModelProperty(value = "数据集标题")
    String dataSetTitle;
    @ApiModelProperty(value = "数据集字段列表")
    List<ColumnSchema> columns;
}
