package com.centit.product.adapter.po;

import com.centit.support.database.orm.GeneratorType;
import com.centit.support.database.orm.ValueGenerator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 数据校验规则
 * @author codefan@sina.com
 */
@ApiModel
@Data
@Entity
@Table(name = "F_DATA_CHECK_RULE")
public class DataCheckRule implements java.io.Serializable {
    private static final long serialVersionUID =  202203291917L;

    @ApiModelProperty(value = "规则ID", hidden = true)
    @Id
    @Column(name = "RULE_ID")
    @ValueGenerator(strategy = GeneratorType.UUID22)
    private String  ruleId;

    @Column(name = "TOP_UNIT")
    @ApiModelProperty(value = "所属租户", name = "topUnit")
    private String topUnit;

    @Column(name = "RULE_TYPE")
    private String  ruleType;

    @Column(name = "RULE_NAME")
    private String  ruleName;

    @Column(name = "RULE_FORMULA")
    private String  ruleFormula;

    @Column(name = "RULE_PARAM_SUM")
    private int  ruleParamSum;

    @Column(name = "RULE_PARAM_DESC")
    private String  ruleParamDesc;

    @Column(name = "FAULT_MESSAGE")
    private String  faultMessage;

    @Column(name = "RULE_DESC")
    private String  ruleDesc;
}
