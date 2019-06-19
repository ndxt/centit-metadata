package com.centit.product.dbdesign.po;

import com.centit.framework.core.dao.DictionaryMap;
import com.centit.support.algorithm.DatetimeOpt;
import com.centit.support.database.orm.GeneratorCondition;
import com.centit.support.database.orm.GeneratorTime;
import com.centit.support.database.orm.GeneratorType;
import com.centit.support.database.orm.ValueGenerator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.hibernate.validator.constraints.Length;

import javax.persistence.*;
import java.util.Date;


/**
 * create by scaffold 2016-06-01


  元数据更改记录null
*/
@ApiModel
@Data
@Entity
@Table(name = "F_META_CHANG_LOG")
public class MetaChangLog implements java.io.Serializable {
    private static final long serialVersionUID =  1L;

    @ApiModelProperty(value = "编号", hidden = true)
    @Id
    @Column(name = "CHANGE_ID")
    @ValueGenerator(strategy = GeneratorType.SEQUENCE, value = "S_META_CHANGLOG_ID")
    private String  changeId;


    @ApiModelProperty(value = "表ID", hidden = true)
    @Column(name = "TABLE_ID")
    private String tableID;

    @ApiModelProperty(value = "数据库ID")
    @Column(name = "DATABASE_CODE")
    private String databaseCode;

    @ApiModelProperty(value = "提交日期", required = true)
    @Column(name = "CHANGE_DATE")
    @ValueGenerator(strategy = GeneratorType.FUNCTION, occasion = GeneratorTime.NEW_UPDATE, condition = GeneratorCondition.ALWAYS, value = "today()")
    @OrderBy(value = "DESC")
    private Date  changeDate;

    @ApiModelProperty(value = "提交人", required = true)
    @Column(name = "CHANGER")
    @DictionaryMap(fieldName = "changerName", value = "userCode")
    private String  changer;

    @ApiModelProperty(value = "更改脚本")
    @Column(name = "CHANGE_SCRIPT")
    private String  changeScript;

    @ApiModelProperty(value = "更改说明")
    @Column(name = "CHANGE_COMMENT")
    @Length(max = 2048, message = "字段长度不能大于{max}")
    private String  changeComment;

    @Transient
    private String changerName;
    // Constructors
    /** default constructor */
    public MetaChangLog() {
        this.changeDate= DatetimeOpt.currentUtilDate();
    }
    /** minimal constructor */
    public MetaChangLog(
        String tableID, String databaseCode
        ,Date  changeDate,String  changer) {

        this.tableID = tableID;
        this.databaseCode = databaseCode;
        this.changeDate= changeDate;
        this.changer= changer;
    }

/** full constructor */
    public MetaChangLog(
     String tableID, String databaseCode
    ,String  changeId,Date  changeDate,String  changer,String  changeScript,String  changeComment) {


        this.tableID = tableID;

        this.changeId= changeId;
        this.databaseCode =databaseCode;
        this.changeDate= changeDate;
        this.changer= changer;
        this.changeScript= changeScript;
        this.changeComment= changeComment;
    }

    public MetaChangLog copy(MetaChangLog other){

        this.setTableID(other.getTableID());

        this.changeId= other.getChangeId();
        this.databaseCode = other.getDatabaseCode();
        this.changeDate= other.getChangeDate();
        this.changer= other.getChanger();
        this.changeScript= other.getChangeScript();
        this.changeComment= other.getChangeComment();
        return this;
    }

    public MetaChangLog copyNotNullProperty(MetaChangLog other){

    if( other.getTableID() != null)
        this.setTableID(other.getTableID());

        if( other.getChangeId() != null)
            this.changeId= other.getChangeId();
        if( other.getDatabaseCode() !=null)
            this.databaseCode= other.getDatabaseCode();
        if( other.getChangeDate() != null)
            this.changeDate= other.getChangeDate();
        if( other.getChanger() != null)
            this.changer= other.getChanger();

        if( other.getChangeScript() != null)
            this.changeScript= other.getChangeScript();
        if( other.getChangeComment() != null)
            this.changeComment= other.getChangeComment();

        return this;
    }

    public MetaChangLog clearProperties(){

        this.changeId= null;
        this.databaseCode= null;
        this.changeDate= null;
        this.changer= null;
        this.changeScript= null;
        this.changeComment= null;
        return this;
    }
}
