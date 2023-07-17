package com.centit.product.metadata.po;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.annotation.JSONField;
import com.centit.framework.core.dao.DictionaryMap;
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
    public static final String CHECK_VALUE_TAG ="checkValue";

    @ApiModelProperty(value = "规则ID", hidden = true)
    @Id
    @Column(name = "RULE_ID")
    @ValueGenerator(strategy = GeneratorType.UUID22)
    private String  ruleId;

    @Column(name = "TOP_UNIT")
    @ApiModelProperty(value = "所属租户", name = "topUnit")
    @DictionaryMap(fieldName = "topUnitName", value = "unitCode")
    private String topUnit;

    @Column(name = "RULE_TYPE")
    @DictionaryMap(fieldName = "ruleTypeText", value = "checkRuleType")
    private String  ruleType;

    @Column(name = "RULE_NAME")
    private String  ruleName;

    @Column(name = "RULE_FORMULA")
    private String  ruleFormula;

    @Column(name = "RULE_PARAM_SUM")
    private int  ruleParamSum;

    @Column(name = "RULE_PARAM_DESC")
    private JSONObject ruleParamDesc;

    @Column(name = "FAULT_MESSAGE")
    private String  faultMessage;

    @Column(name = "RULE_DESC")
    private String  ruleDesc;
    @Column(name = "SOURCE_ID")
    @Length(max = 32, message = "字段长度不能大于{max}")
    @JSONField(serialize = false)
    private String sourceId;
}
