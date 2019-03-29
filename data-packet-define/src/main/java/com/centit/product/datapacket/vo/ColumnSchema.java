package com.centit.product.datapacket.vo;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@Data
public class ColumnSchema{

    @ApiModelProperty(value = "字段代码")
    String columnCode;
    @ApiModelProperty(value = "字段属性名")
    String propertyName;
    @ApiModelProperty(value = "字段名")
    String columnName;
    @ApiModelProperty(value = "字段类型")
    String dataType;
    @ApiModelProperty(value = "是否为统计字段")
    String isStatData;
}
