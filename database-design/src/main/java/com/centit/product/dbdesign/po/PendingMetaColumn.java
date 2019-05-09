package com.centit.product.dbdesign.po;

import com.alibaba.fastjson.annotation.JSONField;
import com.centit.product.metadata.po.MetaColumn;
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
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.NotBlank;

import javax.persistence.*;
import javax.validation.constraints.Pattern;
import java.util.Date;


/**
 * create by scaffold 2016-06-01


  未落实字段元数据表null
*/
@ApiModel
@Data
@Entity
@Table(name = "F_PENDING_META_COLUMN")
public class PendingMetaColumn implements TableField, java.io.Serializable {
    private static final long serialVersionUID =  1L;

//    @EmbeddedId
//    private PendingMetaColumnId cid;

    @ApiModelProperty(value = "表ID", hidden = true)
    @Id
    @Column(name = "TABLE_ID")
    @NotBlank(message = "字段不能为空")
    private String  tableId;

    @ApiModelProperty(value = "字段代码", required = true)
    @Id
    @Column(name = "COLUMN_NAME")
    @NotBlank(message = "字段不能为空")
    private String  columnName;

    @ApiModelProperty(value = "字段名称", required = true)
    @Column(name = "FIELD_LABEL_NAME")
    @NotBlank(message = "字段不能为空")
    @Length(max = 64, message = "字段长度不能大于{max}")
    private String  fieldLabelName;

    @ApiModelProperty(value = "字段描述")
    @Column(name = "COLUMN_COMMENT")
    @Length(max = 256, message = "字段长度不能大于{max}")
    private String  columnComment;

    @ApiModelProperty(value = "显示次序")
    @Column(name = "COLUMN_ORDER")
    private Long  columnOrder;

    @ApiModelProperty(value = "字段类型", required = true)
    @Column(name = "COLUMN_TYPE")
    @NotBlank(message = "字段不能为空")
    @Length(max = 32, message = "字段长度不能大于{max}")
    private String  columnFieldType;

    @ApiModelProperty(value = "字段长度")
    @Column(name = "MAX_LENGTH")
    private Integer  maxLengthM;

    @ApiModelProperty(value = "字段精度")
    @Column(name = "SCALE")
    private Integer  scaleM;

    @ApiModelProperty(value = "字段类别", required = true)
    @Column(name = "ACCESS_TYPE")
    @NotBlank(message = "字段不能为空")
    @Length(message = "字段长度不能大于{max}")
    private String  accessType;

    @ApiModelProperty(value = "是否必填")
    @Column(name = "MANDATORY")
    @Length( message = "字段长度不能大于{max}")
    private String  mandatory;

    @ApiModelProperty(value = "是否为主键")
    @Column(name = "PRIMARYKEY")
    @Length( message = "字段长度不能大于{max}")
    private String  primarykey;

    @ApiModelProperty(value = "状态", required = true)
    @Column(name = "COLUMN_STATE")
    //@NotBlank(message = "字段不能为空")
    @Length( message = "字段长度不能大于{max}")
    private String  columnState;

    @ApiModelProperty(value = "引用类型(0：没有：1： 数据字典 2：JSON表达式 3：sql语句  Y：年份 M：月份)")
    @Column(name = "REFERENCE_TYPE")
    @Length(message = "字段长度不能大于{max}")
    private String  referenceType;

    @ApiModelProperty(value = "引用数据(根据paramReferenceType类型（1,2,3）填写对应值)")
    @Column(name = "REFERENCE_DATA")
    @Length(max = 1000, message = "字段长度不能大于{max}")
    private String  referenceData;

    @ApiModelProperty(value = "约束表达式(regex表达式)")
    @Column(name = "VALIDATE_REGEX")
    @Length(max = 200, message = "字段长度不能大于{max}")
    private String  validateRegex;

    @ApiModelProperty(value = "约束提示(约束不通过提示信息)")
    @Column(name = "VALIDATE_INFO")
    @Length(max = 200, message = "字段长度不能大于{max}")
    private String  validateInfo;

    @ApiModelProperty(value = "自动生成规则(C 常量  U uuid S sequence)")
    @Column(name = "AUTO_CREATE_RULE")
    @Length(max = 200, message = "字段长度不能大于{max}")
    private String  autoCreateRule;

    @ApiModelProperty(value = "自动生成参数")
    @Column(name = "AUTO_CREATE_PARAM")
    @Length(max = 200, message = "字段长度不能大于{max}")
    private String  autoCreateParam;

    @ApiModelProperty(value = "与流程中业务关联关系(0: 不是工作流变量 1：流程业务变量 2： 流程过程变量)")
    @Column(name = "WORKFLOW_VARIABLE_TYPE")
    @Pattern(regexp = "[0-2]")
    @Length(max = 1, message = "字段长度不能大于{max}")
    private String workFlowVariableType;

    @ApiModelProperty(value = "更改时间", hidden = true)
    @ValueGenerator(strategy = GeneratorType.FUNCTION, occasion = GeneratorTime.NEW_UPDATE, condition = GeneratorCondition.ALWAYS, value = "today()")
    @Column(name = "LAST_MODIFY_DATE")
    private Date  lastModifyDate;

    @ApiModelProperty(value = "更改人员")
    @Column(name = "RECORDER")
    @Length(max = 8, message = "字段长度不能大于{max}")
    private String  recorder;

    @Transient
    private DBType databaseType;

    // Constructors
    /** default constructor */
    public PendingMetaColumn() {
        this.columnState="0";
    }

    public PendingMetaColumn(PendingMetaTable mdTable, String columnName) {
//        this.cid= new PendingMetaColumnId(mdTable,columnName);
        this.tableId = mdTable.getTableId();
        this.columnName = columnName;
        this.columnState="0";
    }

    /** minimal constructor */
//    public PendingMetaColumn(
//        PendingMetaColumnId cid,String  fieldLabelName,String  columnType,String  accessType,String  columnState) {
//        this.cid=cid;
//        this.fieldLabelName= fieldLabelName;
//        this.columnFieldType= columnType;
//        this.accessType= accessType;
//        this.columnState= columnState;
//    }

/** full constructor */
    public PendingMetaColumn(String  fieldLabelName,String  columnComment,Long  columnOrder,String  columnType,
            Integer  maxLength,Integer  scale,String  accessType,String  mandatory,String  primarykey,String  columnState,String  referenceType,String  referenceData,String  validateRegex,String  validateInfo,String  defaultValue,Date  lastModifyDate,String  recorder) {

//        this.cid=cid;
        this.fieldLabelName= fieldLabelName;
        this.columnComment= columnComment;
        this.columnOrder= columnOrder;
        this.columnFieldType= columnType;
        this.maxLengthM= maxLength;
        this.scaleM= scale;
        this.accessType= accessType;
        this.mandatory= mandatory;
        this.primarykey= primarykey;
        this.columnState= columnState;
        this.referenceType= referenceType;
        this.referenceData= referenceData;
        this.validateRegex= validateRegex;
        this.validateInfo= validateInfo;
        this.lastModifyDate= lastModifyDate;
        this.recorder= recorder;
    }

    public PendingMetaColumn copy(PendingMetaColumn other){

//        this.setCid(other.getCid());
        this.fieldLabelName= other.getFieldLabelName();
        this.columnComment= other.getColumnComment();
        this.columnOrder= other.getColumnOrder();
        this.columnFieldType= other.getColumnFieldType();
        this.maxLengthM= other.getMaxLengthM();
        this.scaleM= other.getScaleM();
        this.accessType= other.getAccessType();
        this.mandatory= other.isMandatory()?"T":"F";
        this.primarykey= other.getPrimarykey();
        this.columnState= other.getColumnState();
        this.referenceType= other.getReferenceType();
        this.referenceData= other.getReferenceData();
        this.validateRegex= other.getValidateRegex();
        this.validateInfo= other.getValidateInfo();
        this.lastModifyDate= other.getLastModifyDate();
        this.recorder= other.getRecorder();

        return this;
    }

    public PendingMetaColumn copyNotNullProperty(PendingMetaColumn other){
//        if( other.getCid() != null)
//            this.setCid(other.getCid());
        if( other.getFieldLabelName() != null)
            this.fieldLabelName= other.getFieldLabelName();
        if( other.getColumnComment() != null)
            this.columnComment= other.getColumnComment();
        if( other.getColumnOrder() != null)
            this.columnOrder= other.getColumnOrder();
        if( other.getColumnType() != null)
            this.columnFieldType= other.getColumnFieldType();
        if( other.getMaxLengthM() != null)
            this.maxLengthM= other.getMaxLengthM();
        if( other.getScaleM() != null)
            this.scaleM= other.getScaleM();
        if( other.getAccessType() != null)
            this.accessType= other.getAccessType();
        if( other.getMandatory() != null)
            this.mandatory= other.isMandatory()?"T":"F";
        if( other.getPrimarykey() != null)
            this.primarykey= other.getPrimarykey();
        if( other.getColumnState() != null)
            this.columnState= other.getColumnState();
        if( other.getReferenceType() != null)
            this.referenceType= other.getReferenceType();
        if( other.getReferenceData() != null)
            this.referenceData= other.getReferenceData();
        if( other.getValidateRegex() != null)
            this.validateRegex= other.getValidateRegex();
        if( other.getValidateInfo() != null)
            this.validateInfo= other.getValidateInfo();
        if( other.getLastModifyDate() != null)
            this.lastModifyDate= other.getLastModifyDate();
        if( other.getRecorder() != null)
            this.recorder= other.getRecorder();

        return this;
    }

    public PendingMetaColumn clearProperties(){
//        this.setCid(null);
        this.fieldLabelName= null;
        this.columnComment= null;
        this.columnOrder= null;
        this.columnFieldType= null;
        this.accessType= null;
        this.mandatory= null;
        this.primarykey= null;
        this.columnState= null;
        this.referenceType= null;
        this.referenceData= null;
        this.validateRegex= null;
        this.validateInfo= null;
        this.lastModifyDate= null;
        this.recorder= null;

        return this;
    }
    @Override
    public String getPropertyName() {
        return FieldType.mapPropName(getColumnName());
    }
    @Override
    public String getJavaType() {
        return FieldType.mapToJavaType(this.columnFieldType, this.scaleM==null?0:this.scaleM);
    }
    @Override
    public boolean isMandatory() {
        return "T".equals(mandatory) ||  "Y".equals(mandatory) || "1".equals(mandatory);
    }

    public boolean isPrimaryKey() {
        return "T".equals(primarykey) ||  "Y".equals(primarykey) || "1".equals(primarykey);
    }


    @Override
    public int getMaxLength() {
        if("string".equalsIgnoreCase(this.columnFieldType) ||
                "integer".equalsIgnoreCase(this.columnFieldType)||
                "float".equalsIgnoreCase(this.columnFieldType) ||
                "varchar".equalsIgnoreCase(this.columnFieldType)||
                "number".equalsIgnoreCase(this.columnFieldType))
            return maxLengthM==null?0:maxLengthM.intValue();
        return 0;
    }
    @Override
    public int getPrecision() {
        return getMaxLength();
    }
    @Override
    public int getScale() {
        if("float".equalsIgnoreCase(this.columnFieldType) ||
                "number".equalsIgnoreCase(this.columnFieldType))
            return scaleM==null?0:scaleM.intValue();
        return 0;
    }

    @Override
    public String getDefaultValue() {
        return "C".equals(autoCreateRule)?autoCreateParam:null;
    }
    @Override
    @JSONField(serialize=false)
    public String getColumnType() {
        return FieldType.mapToDatabaseType(this.columnFieldType, this.databaseType);
    }

    public MetaColumn mapToMetaColumn(){
        MetaColumn mc = new MetaColumn();
        mc.setTableId(this.getTableId());
        mc.setColumnName(this.getColumnName());
        mc.setFieldLabelName(this.getFieldLabelName());
        mc.setColumnComment(this.getColumnComment());
        mc.setColumnOrder(this.getColumnOrder());
        mc.setColumnType(getColumnFieldType());
        mc.setColumnLength(this.getMaxLength());
        mc.setColumnPrecision(this.getScale());
        mc.setAccessType(this.getAccessType());
        mc.setMandatory(this.isMandatory()?"T":"F");
        mc.setPrimaryKey(this.getPrimarykey());
        mc.setColumnState(this.getColumnState());
        mc.setReferenceType(this.getReferenceType());
        mc.setReferenceData(this.getReferenceData());
        mc.setValidateRegex(this.getValidateRegex());
        mc.setValidateInfo(this.getValidateInfo());
        mc.setAutoCreateRule(this.getAutoCreateRule());
        mc.setAutoCreateParam(this.getAutoCreateParam());
        mc.setLastModifyDate(this.getLastModifyDate());
        mc.setRecorder(this.getRecorder());
        mc.setWorkFlowVariableType(this.getWorkFlowVariableType());
        return mc;
    }
}
