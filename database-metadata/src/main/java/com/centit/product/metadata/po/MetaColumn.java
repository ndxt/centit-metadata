package com.centit.product.metadata.po;

import com.alibaba.fastjson.annotation.JSONField;
import com.centit.framework.core.dao.DictionaryMap;
import com.centit.support.database.metadata.SimpleTableField;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.orm.GeneratorCondition;
import com.centit.support.database.orm.GeneratorTime;
import com.centit.support.database.orm.GeneratorType;
import com.centit.support.database.orm.ValueGenerator;
import com.centit.support.database.utils.DBType;
import com.centit.support.database.utils.FieldType;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.Length;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import java.util.Date;

/**
 * @author codefan
 * @author zouwy
*/
@ApiModel
@Data
@NoArgsConstructor
@Entity
@Table(name = "F_MD_COLUMN")
public class MetaColumn implements TableField,java.io.Serializable {
    private static final long serialVersionUID =  201901071109L;

    @ApiModelProperty(value = "表ID", hidden = true)
    @Id
    @Column(name = "TABLE_ID")
    @NotBlank(message = "字段不能为空")
    private String tableId;

    @ApiModelProperty(value = "字段名", hidden = true)
    @Id
    @Column(name = "COLUMN_NAME")
    @NotBlank(message = "字段不能为空")
    private String columnName;

    @ApiModelProperty(value = "字段显示名（可编辑）")
    @Column(name = "FIELD_LABEL_NAME")
    @NotBlank(message = "字段不能为空")
    @Length(max = 64, message = "字段长度不能大于{max}")
    private String fieldLabelName;

    @ApiModelProperty(value = "字段描述（可编辑）")
    @Column(name = "COLUMN_COMMENT")
    @Length(max = 256, message = "字段长度不能大于{max}")
    private String  columnComment;

    @ApiModelProperty(value = "显示顺序（可编辑）")
    @Column(name = "COLUMN_ORDER")
    private Long columnOrder;

    @ApiModelProperty(value = "字段类型", hidden = true)
    @Column(name = "COLUMN_TYPE")
    @NotBlank(message = "字段不能为空")
    @Length(max = 32, message = "字段长度不能大于{max}")
    private String columnType;

    @ApiModelProperty(value = "字段类型", hidden = true)
    @Column(name = "FIELD_TYPE")
    @NotBlank(message = "字段不能为空")
    @Length(max = 32, message = "字段长度不能大于{max}")
    private String fieldType;

    @ApiModelProperty(value = "字段长度", hidden = true)
    @Column(name = "COLUMN_LENGTH")
    private Integer columnLength;

    @ApiModelProperty(value = "字段精度", hidden = true)
    @Column(name = "SCALE")
    private Integer scale;

    /**
     * 字段类别 控制自定义表单中是否可以 访问字段
     * 比如： 最后修改时间、工作流相关的字段、更新版本号，自定义表单中就不用显示
     */
    @ApiModelProperty(value = "字段类别（可编辑）(字段类别.H：隐藏；R：只读；C：只能创建不能修改；N：可读写)")
    @Column(name = "ACCESS_TYPE")
    @NotBlank(message = "字段不能为空")
    @Pattern(regexp = "[HRCN]")
    @Length(  message = "字段长度不能大于{max}")
    private String  accessType;

    /**
     * 是否必填 T-F
     */
    @ApiModelProperty(value = "是否必填", hidden = true)
    @Column(name = "MANDATORY")
    @Pattern(regexp = "[TF]")
    @Length(  message = "字段长度不能大于{max}")
    private String  mandatory;

    /**
     * 是否为主键
     */
    @ApiModelProperty(value = "是否主键", hidden = true)
    @Column(name = "PRIMARY_KEY")
    @Pattern(regexp = "[TF]")
    @Length(  message = "字段长度不能大于{max}")
    private String primaryKey;

    /**
     * 引用类型 0：没有：1： 数据字典 2：JSON表达式 3：sql语句  Y：年份 M：月份
     */
    @ApiModelProperty(value = "引用类型 0：没有：1： 数据字典 2：JSON表达式 3：sql语句  Y：年份 M：月份")
    @Column(name = "REFERENCE_TYPE")
    @Length(message = "字段长度不能大于{max}")
    private String  referenceType;
    /**
     * 引用数据 根据paramReferenceType类型（1,2,3）填写对应值
     */
    @ApiModelProperty(value = "引用数据 根据paramReferenceType类型（1,2,3）填写对应值")
    @Column(name = "REFERENCE_DATA")
    @Length(max = 1000, message = "字段长度不能大于{max}")
    private String  referenceData;
    /**
     * 约束表达式 regex表达式
     */
    @ApiModelProperty(value = "约束表达式 regex表达式")
    @Column(name = "VALIDATE_REGEX")
    @Length(max = 200, message = "字段长度不能大于{max}")
    private String  validateRegex;
    /**
     * 约束提示 约束不通过提示信息
     */
    @ApiModelProperty(value = "约束提示 约束不通过提示信息")
    @Column(name = "VALIDATE_INFO")
    @Length(max = 200, message = "字段长度不能大于{max}")
    private String  validateInfo;

    /**
     * 自动生成规则   C 常量  U uuid S sequence
     */
    @ApiModelProperty(value = "自动生成规则   C 常量  U uuid S sequence F 函数")
    @Column(name = "AUTO_CREATE_RULE")
    @Length(max = 200, message = "字段长度不能大于{max}")
    private String  autoCreateRule;

    /**
     * 自动生成参数
     */
    @ApiModelProperty(value = "自动生成参数")
    @Column(name = "AUTO_CREATE_PARAM")
    @Length(max = 200, message = "字段长度不能大于{max}")
    private String  autoCreateParam;

    /**
     * 与流程中变量关联关系
     * 0: 不是流程变量 1：流程业务变量 2： 流程过程变量
     */
    @Column(name = "WORKFLOW_VARIABLE_TYPE")
    @Pattern(regexp = "[0-2]")
    @Length(max = 1, message = "字段长度不能大于{max}")
    private String workFlowVariableType;

    /**
     * 更改时间
     */
    @ApiModelProperty(hidden = true)
    @ValueGenerator(strategy = GeneratorType.FUNCTION, occasion = GeneratorTime.NEW_UPDATE, condition = GeneratorCondition.ALWAYS, value = "today()")
    @Column(name = "LAST_MODIFY_DATE")
    private Date lastModifyDate;
    /**
     * 更改人员 null
     */
    @ApiModelProperty(value = "更改人员", hidden = true)
    @Column(name = "RECORDER")
    @Length(max = 8, message = "字段长度不能大于{max}")
    @DictionaryMap(fieldName = "recorderName", value = "userCode")
    private String  recorder;

    @Transient
    @ApiModelProperty(hidden = true)
    private DBType databaseType;

    public MetaColumn(@NotBlank(message = "字段不能为空") String tableId, @NotBlank(message = "字段不能为空") String columnName) {
        this.tableId = tableId;
        this.columnName = columnName;
    }

    public MetaColumn convertFromTableField(SimpleTableField tableField){
        this.columnName = tableField.getColumnName();
        this.columnType = tableField.getColumnType();
        this.fieldType =  tableField.getFieldType();
        if(StringUtils.isNotBlank(tableField.getFieldLabelName())){
            this.fieldLabelName = tableField.getFieldLabelName();
        }
        if(StringUtils.isNotBlank(tableField.getColumnComment()) && StringUtils.isBlank(this.columnComment)){
            this.columnComment = tableField.getColumnComment();
        }
        this.columnLength = tableField.getMaxLength();
        this.scale = tableField.getScale();
        this.mandatory = tableField.isMandatory() ? "T" : "F";
        this.accessType = StringUtils.isNotBlank(this.accessType) ? this.accessType : "N";
        return this;
    }

    /**
     * 这个是用于生产数据库表创建语句的，不是用来生成表单默认值的
     */
    @ApiModelProperty(hidden = true)
    @JSONField(serialize = false)
    @Override
    public String getDefaultValue() {
        return null;
    }

    @ApiModelProperty(hidden = true)
    @Override
    public String getPropertyName() {
        return FieldType.mapPropName(this.columnName);
    }

    @Override
    public boolean isMandatory() {
        return "T".equals(mandatory);
    }

    public boolean isPrimaryKey() {
        return "T".equals(primaryKey);
    }

    public Integer getColumnLength() {
        if(FieldType.STRING.equalsIgnoreCase(this.fieldType) ||
            FieldType.FLOAT.equalsIgnoreCase(this.fieldType) ||
            FieldType.DOUBLE.equalsIgnoreCase(this.fieldType)||
            FieldType.MONEY.equalsIgnoreCase(this.fieldType) ||
            FieldType.INTEGER.equalsIgnoreCase(this.fieldType))
            return columnLength;
        return 0;
    }

    public void setColumnLength(Integer columnLength){
        this.columnLength = columnLength;
    }


    @Override
    public Integer getPrecision() {
        return getColumnLength();
    }

    @Override
    @ApiModelProperty(hidden = true)
    public Integer getScale() {
        if(FieldType.FLOAT.equalsIgnoreCase(this.fieldType) ||
            FieldType.DOUBLE.equalsIgnoreCase(this.fieldType)||
            FieldType.MONEY.equalsIgnoreCase(this.fieldType) ||
            FieldType.INTEGER.equalsIgnoreCase(this.fieldType))
            return scale;
        return 0;
    }


    @Override
    public String getColumnType() {
        return FieldType.mapToDatabaseType(this.columnType,this.databaseType);
    }


    @Override
    @ApiModelProperty(hidden = true)
    @JSONField(serialize = false)
    public Integer getMaxLength() {
        return this.getColumnLength();
    }

    public String getJavaTypeFullName(){
        String javaType = FieldType.mapToJavaType(fieldType, scale);
        if ("Date".equals(javaType))
            return "java.util." + javaType;
        if ("Timestamp".equals(javaType))
            return "java.sql.Timestamp";
        if ("byte[]".equals(javaType)) {
            return javaType;
        }
        return "java.lang." + javaType;
    }
}
