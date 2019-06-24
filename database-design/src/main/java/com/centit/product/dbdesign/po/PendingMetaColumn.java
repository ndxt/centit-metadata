package com.centit.product.dbdesign.po;

import com.alibaba.fastjson.annotation.JSONField;
import com.centit.product.metadata.po.MetaColumn;
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
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.Length;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
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

    @ApiModelProperty(value = "字段类型", required = true)
    @Column(name = "FIELD_TYPE")
    @NotBlank(message = "字段不能为空")
    @Length(max = 32, message = "字段长度不能大于{max}")
    private String fieldType;

    @ApiModelProperty(value = "字段长度")
    @Column(name = "MAX_LENGTH")
    private Integer  maxLengthM;

    @ApiModelProperty(value = "字段精度")
    @Column(name = "SCALE")
    private Integer scale;

    @ApiModelProperty(value = "显示次序")
    @Column(name = "COLUMN_ORDER")
    private Long  columnOrder;

    @ApiModelProperty(value = "是否必填")
    @Column(name = "MANDATORY")
    @Length( message = "字段长度不能大于{max}")
    private String  mandatory;

    @ApiModelProperty(value = "是否为主键")
    @Column(name = "PRIMARYKEY")
    @Length( message = "字段长度不能大于{max}")
    private String  primarykey;

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

    }

    public PendingMetaColumn(PendingMetaTable mdTable, String columnName) {
//        this.cid= new PendingMetaColumnId(mdTable,columnName);
        this.tableId = mdTable.getTableId();
        this.columnName = columnName;
    }

    /** minimal constructor */
//    public PendingMetaColumn(
//        PendingMetaColumnId cid,String  fieldLabelName,String  columnType,String  accessType,String  columnState) {
//        this.cid=cid;
//        this.fieldLabelName= fieldLabelName;
//        this.fieldType= columnType;
//        this.accessType= accessType;
//        this.columnState= columnState;
//    }

/** full constructor */
    public PendingMetaColumn(String  fieldLabelName,String  columnComment,Long  columnOrder,String  columnType,
            Integer  maxLength,Integer  scale,String  mandatory,String  primarykey,String  defaultValue,Date  lastModifyDate,String  recorder) {

//        this.cid=cid;
        this.fieldLabelName= fieldLabelName;
        this.columnComment= columnComment;
        this.columnOrder= columnOrder;
        this.fieldType = columnType;
        this.maxLengthM= maxLength;
        this.scale = scale;
        this.mandatory= mandatory;
        this.primarykey= primarykey;
        this.lastModifyDate= lastModifyDate;
        this.recorder= recorder;
    }

    public PendingMetaColumn copy(PendingMetaColumn other){

//        this.setCid(other.getCid());
        this.fieldLabelName= other.getFieldLabelName();
        this.columnComment= other.getColumnComment();
        this.columnOrder= other.getColumnOrder();
        this.fieldType = other.getFieldType();
        this.maxLengthM= other.getMaxLengthM();
        this.scale = other.getScale();
        this.mandatory= other.isMandatory()?"T":"F";
        this.primarykey= other.getPrimarykey();
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
            this.fieldType = other.getFieldType();
        if( other.getMaxLengthM() != null)
            this.maxLengthM= other.getMaxLengthM();
        if( other.getScale() != null)
            this.scale = other.getScale();
        if( other.getMandatory() != null)
            this.mandatory= other.isMandatory()?"T":"F";
        if( other.getPrimarykey() != null)
            this.primarykey= other.getPrimarykey();
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
        this.fieldType = null;
        this.mandatory= null;
        this.primarykey= null;
        this.lastModifyDate= null;
        this.recorder= null;

        return this;
    }
    @Override
    public String getPropertyName() {
        return FieldType.mapPropName(getColumnName());
    }

    @Override
    public String  getFieldType() {
        return fieldType;
    }

    @Override
    public boolean isMandatory() {
        return "T".equals(mandatory) ||  "Y".equals(mandatory) || "1".equals(mandatory);
    }

    public boolean isPrimaryKey() {
        return "T".equals(primarykey) ||  "Y".equals(primarykey) || "1".equals(primarykey);
    }


    @Override
    public Integer getMaxLength() {
        if(FieldType.STRING.equalsIgnoreCase(this.fieldType) ||
            FieldType.FLOAT.equalsIgnoreCase(this.fieldType) ||
            FieldType.DOUBLE.equalsIgnoreCase(this.fieldType)||
            FieldType.MONEY.equalsIgnoreCase(this.fieldType) ||
            FieldType.INTEGER.equalsIgnoreCase(this.fieldType))
            return maxLengthM==null?0:maxLengthM.intValue();
        return 0;
    }
    @Override
    public Integer getPrecision() {
        return getMaxLength();
    }

    @Override
    public Integer getScale() {
        if(FieldType.FLOAT.equalsIgnoreCase(this.fieldType) ||
            FieldType.DOUBLE.equalsIgnoreCase(this.fieldType)||
            FieldType.MONEY.equalsIgnoreCase(this.fieldType)||
            FieldType.INTEGER.equalsIgnoreCase(this.fieldType))
            return scale ==null?0: scale.intValue();
        return 0;
    }

    @Override
    public String getDefaultValue() {
        return null;
    }

    @Override
    @JSONField(serialize=false)
    public String getColumnType() {
        return FieldType.mapToDatabaseType(this.fieldType, this.databaseType);
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

    public MetaColumn mapToMetaColumn(){
        MetaColumn mc = new MetaColumn();
        mc.setDatabaseType(this.getDatabaseType());
        mc.setTableId(this.getTableId());
        mc.setColumnName(this.getColumnName());
        mc.setFieldLabelName(this.getFieldLabelName());
        mc.setColumnComment(this.getColumnComment());
        mc.setColumnOrder(this.getColumnOrder());
        mc.setColumnType(this.getColumnType());
        mc.setFieldType(this.getFieldType());
        mc.setColumnLength(this.getMaxLength());
        mc.setScale(this.getScale());
        mc.setAccessType("N");
        mc.setMandatory(this.isMandatory()?"T":"F");
        mc.setPrimaryKey(this.getPrimarykey()==null?"F":this.getPrimarykey());
        mc.setLastModifyDate(this.getLastModifyDate());
        mc.setRecorder(this.getRecorder());
        return mc;
    }

    public PendingMetaColumn convertFromTableField(SimpleTableField tableField){
        this.columnName = tableField.getColumnName();
        this.fieldType = tableField.getFieldType();
        if(StringUtils.isNotBlank(tableField.getFieldLabelName())){
            this.fieldLabelName = tableField.getFieldLabelName();
        }
        if(StringUtils.isNotBlank(tableField.getColumnComment()) && StringUtils.isBlank(this.columnComment)){
            this.columnComment = tableField.getColumnComment();
        }
        this.maxLengthM = tableField.getMaxLength();
        this.scale = tableField.getScale();
        this.mandatory = tableField.isMandatory() ? "T" : "F";
        return this;
    }
}
