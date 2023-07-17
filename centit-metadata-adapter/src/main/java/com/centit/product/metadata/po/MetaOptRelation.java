package com.centit.product.metadata.po;

import com.centit.support.database.orm.GeneratorType;
import com.centit.support.database.orm.ValueGenerator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

/**
 *关联元数据表
 */
@ApiModel
@Data
@Entity
@Table(name = "f_table_opt_relation")
public class MetaOptRelation  implements Serializable {
    private static final long serialVersionUID = 1L;

    @ApiModelProperty(value = "主键id")
    @Id
    @Column(name = "id")
    @ValueGenerator(strategy = GeneratorType.UUID)
    private String id;


    @ApiModelProperty(value = "表ID", hidden = true)
    @Column(name = "table_id")
    private String tableId;


    @ApiModelProperty(value = "业务编号")
    @Column(name = "OPT_ID")
    @Length(max = 32, message = "字段长度不能大于{max}")
    private String optId;


    @ApiModelProperty(value = "应用id")
    @Column(name = "os_id")
    @Length(max = 32, message = "字段长度不能大于{max}")
    private String osId;


}
