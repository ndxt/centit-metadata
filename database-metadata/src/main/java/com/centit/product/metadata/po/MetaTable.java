package com.centit.product.metadata.po;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.centit.framework.components.CodeRepositoryUtil;
import com.centit.framework.core.dao.DictionaryMap;
import com.centit.framework.core.dao.DictionaryMapColumn;
import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.product.metadata.service.impl.SqlDictionaryMapSupplier;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.common.CachedObject;
import com.centit.support.common.ICachedObject;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.metadata.TableInfo;
import com.centit.support.database.metadata.TableReference;
import com.centit.support.database.orm.GeneratorCondition;
import com.centit.support.database.orm.GeneratorType;
import com.centit.support.database.orm.ValueGenerator;
import com.centit.support.database.utils.DBType;
import com.centit.support.security.Md5Encoder;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.Length;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author codefan
 * @author zouwy
 */
@Data
@ApiModel(value = "表元数据")
@Entity
@Table(name = "F_MD_TABLE")
public class MetaTable implements TableInfo, java.io.Serializable {
    private static final long serialVersionUID = 1L;
    public static final String OBJECT_AS_CLOB_ID_FIELD = "OBJECT_ID";
    public static final String OBJECT_AS_CLOB_ID_PROP = "objectId";
    public static final String OBJECT_AS_CLOB_FIELD = "OBJECT_JSON";
    public static final String OBJECT_AS_CLOB_PROP = "objectJson";
    public static final String UPDATE_CHECK_TIMESTAMP_FIELD = "LAST_MODIFY_TIME";
    public static final String UPDATE_CHECK_TIMESTAMP_PROP = "lastModifyTime";
    public static final String WORKFLOW_INST_ID_FIELD = "FLOW_INST_ID";
    public static final String WORKFLOW_INST_ID_PROP = "flowInstId";
    public static final String WORKFLOW_NODE_INST_ID_FIELD = "NODE_INST_ID";
    public static final String WORKFLOW_NODE_INST_ID_PROP = "nodeInstId";

    /**
     * 主键前缀
     */
    private static final String PRIMARY_KEY_PREFIX = "PK_";

    /**
     * 表ID 表编号
     */
    @Id
    @Column(name = "TABLE_ID")
    @ApiModelProperty(value = "表ID")
    @ValueGenerator(strategy = GeneratorType.UUID22)
    private String tableId;

    /**
     * 所属数据库ID
     */
    @Column(name = "DATABASE_CODE")
    @ApiModelProperty(value = "数据库ID")
    private String databaseCode;

    /**
     * 类别 表 T table /视图 V view / C-大字段 目前只支持json格式
     */
    @Column(name = "TABLE_TYPE")
    @NotBlank(message = "字段不能为空")
    @Pattern(regexp = "[TVC]")
    @Length(max = 1, message = "字段长度不能大于{max}")
    @ApiModelProperty(value = "表类别（T-表；V-视图；C-大字段）")
    @DictionaryMap(fieldName = "tableTypeText", value = "TableType")
    private String tableType;

    @ApiModelProperty(value = "表的存储类别  H：隐藏；R：只读；N：可读写)")
    @Column(name = "ACCESS_TYPE")
    @NotBlank(message = "字段不能为空")
    @Pattern(regexp = "[HRCN]")
    @Length(max = 1, message = "字段长度不能大于{max}")
    private String  accessType;

    /**
     * 表代码/表名
     */
    @Column(name = "TABLE_NAME")
    @NotBlank(message = "字段不能为空")
    @Length(max = 64, message = "字段长度不能大于{max}")
    @ApiModelProperty(value = "表名")
    private String tableName;

    /**
     * 表中文名称
     */
    @Column(name = "TABLE_LABEL_NAME")
    @NotBlank(message = "字段不能为空")
    @Length(max = 100, message = "字段长度不能大于{max}")
    @ApiModelProperty(value = "表中文名")
    private String tableLabelName;

    /**
     * 描述
     */
    @ApiModelProperty(value = "表描述")
    @Column(name = "TABLE_COMMENT")
    @Length(max = 256, message = "字段长度不能大于{max}")
    private String tableComment;
    /**
     * 与流程中业务关联关系
     * 0: 不关联工作流 1：和流程业务关联 2： 和流程过程关联
     */
    @Column(name = "WORKFLOW_OPT_TYPE")
    @NotBlank(message = "字段不能为空")
    @Pattern(regexp = "[0-2]")
    @Length(max = 1, message = "字段长度不能大于{max}")
    private String workFlowOptType;

    @Column(name = "FULLTEXT_SEARCH")
    @NotBlank(message = "字段不能为空[T/F]")
    @Length(max = 1, message = "字段长度不能大于{max}")
    private Boolean fulltextSearch;

    /**
     * 对象标题模板，用于全文检索时的标题显示
     */
    @ApiModelProperty(value = "对象标题模板，用于全文检索时的标题显示")
    @Column(name = "OBJECT_TITLE")
    @Length(max = 500, message = "字段长度不能大于{max}")
    private String  objectTitle;

    @Column(name = "WRITE_OPT_LOG")
    @NotBlank(message = "字段不能为空[T/F]")
    @Length(max = 1, message = "字段长度不能大于{max}")
    private Boolean writeOptLog;

    //T/F 更新时是否校验时间戳 添加 lastModifyTime datetime
    @Column(name = "UPDATE_CHECK_TIMESTAMP")
    @NotBlank(message = "字段不能为空[T/F]")
    @Length(max = 1, message = "字段长度不能大于{max}")
    private Boolean updateCheckTimeStamp;

    /**
     * 更改时间
     */
    @Column(name = "RECORD_DATE")
    @ValueGenerator(strategy = GeneratorType.FUNCTION, condition = GeneratorCondition.ALWAYS, value = "today()")
    private Date recordDate;

    /**
     * 更改人员
     */
    @Column(name = "RECORDER")
    @Length(max = 64, message = "字段长度不能大于{max}")
    @DictionaryMap(fieldName = "recorderName", value = "userCode")
    private String recorder;

    @OneToMany(targetEntity = MetaColumn.class)
    @JoinColumn(name = "TABLE_ID", referencedColumnName = "TABLE_ID")
    private List<MetaColumn> mdColumns;

    @OneToMany(targetEntity = MetaRelation.class)
    @JoinColumn(name = "tableId", referencedColumnName = "parentTableId")
    private List<MetaRelation> mdRelations;

    @OneToMany(targetEntity = MetaRelation.class)
    @JoinColumn(name = "tableId", referencedColumnName = "childTableId")
    private List<MetaRelation> parents;

    @Transient
    @ApiModelProperty(hidden = true)
    private DBType databaseType;

    public void setDatabaseType(DBType databaseType) {
        this.databaseType = databaseType;
        if (this.mdColumns != null) {
            for (MetaColumn col : this.mdColumns) {
                col.setDatabaseType(databaseType);
            }
        }
    }

    public List<MetaColumn> getMdColumns() {
        if (this.mdColumns == null)
            this.mdColumns = new ArrayList<>();
        return this.mdColumns;
    }

    public void addMdColumn(MetaColumn mdColumn) {
        if (this.mdColumns == null)
            this.mdColumns = new ArrayList<>();
        this.mdColumns.add(mdColumn);
    }

    public void removeMdColumn(MetaColumn mdColumn) {
        if (this.mdColumns == null)
            return;
        this.mdColumns.remove(mdColumn);
    }

    public MetaColumn newMdColumn() {
        MetaColumn res = new MetaColumn();

        res.setTableId(this.getTableId());

        return res;
    }

    public List<MetaRelation> getParents() {
        if (this.parents == null)
            this.parents = new ArrayList<>(4);
        return this.parents;
    }

    public void addParent(MetaRelation parent) {
        this.getParents().add(parent);
    }

    public void removeParent(MetaRelation parent) {
        if (this.parents == null)
            return;
        this.parents.remove(parent);
    }

    public List<MetaRelation> getMdRelations() {
        if (this.mdRelations == null)
            this.mdRelations = new ArrayList<>(4);
        return this.mdRelations;
    }


    public void addMdRelation(MetaRelation mdRelation) {
        this.getMdRelations().add(mdRelation);
    }

    public void removeMdRelation(MetaRelation mdRelation) {
        if (this.mdRelations == null)
            return;
        this.mdRelations.remove(mdRelation);
    }

    public MetaTable(){
        this.accessType = "N";
        this.workFlowOptType = "0";
        this.fulltextSearch =  false;
        this.writeOptLog = false;
        this.updateCheckTimeStamp = false;
    }

    public boolean isWriteOptLog(){
        return writeOptLog != null && writeOptLog;
    }

    public boolean isUpdateCheckTimeStamp(){
        return updateCheckTimeStamp != null && updateCheckTimeStamp;
    }
    public boolean isFulltextSearch(){
        return fulltextSearch != null && fulltextSearch;
    }

    //将数据库表同步到元数据表
    public MetaTable convertFromDbTable(SimpleTableInfo tableInfo){

        this.tableName = tableInfo.getTableName();
        if(StringUtils.isNotBlank(tableInfo.getTableLabelName())) {
            this.tableLabelName = tableInfo.getTableLabelName();
        }
        if(StringUtils.isNotBlank(tableInfo.getTableComment())){
            this.tableComment = tableInfo.getTableComment();
        }
        this.tableType = tableInfo.getTableType();
        this.accessType = StringUtils.isBlank(this.accessType) ? "N" : this.accessType;
        return this;
    }

    @Override
    @ApiModelProperty(hidden = true)
    @JSONField(serialize = false)
    public String getPkName() {
        return PRIMARY_KEY_PREFIX + this.tableLabelName;
    }

    @Override
    @ApiModelProperty(hidden = true)
    public String getSchema() {
        return null;
    }

    /**
     * @return 默认排序语句
     */
    @Override
    @ApiModelProperty(hidden = true)
    public String getOrderBy() {
        return null;
    }

    @Override
    public MetaColumn findFieldByName(String name) {
        if (mdColumns == null)
            return null;
        for (MetaColumn c : mdColumns) {
            if (c.getPropertyName().equalsIgnoreCase(name)||
                c.getFieldLabelName().equalsIgnoreCase(name))
                return c;
        }
        return null;
    }

    @Override
    public MetaColumn findFieldByColumn(String name) {
        if (mdColumns == null)
            return null;
        for (MetaColumn c : mdColumns) {
            if (c.getColumnName().equalsIgnoreCase(name))
                return c;
        }
        for (MetaColumn c : mdColumns) {
            if (c.getPropertyName().equalsIgnoreCase(name))
                return c;
        }
        return null;
    }

    @Override
    @ApiModelProperty(hidden = true)
    public List<MetaColumn> getColumns() {
        return mdColumns;
    }

    @Override
    @ApiModelProperty(hidden = true)
    public List<? extends TableReference> getReferences() {
        return this.mdRelations;
    }

    @Override
    @ApiModelProperty(hidden = true)
    public String getTableLabelName() {
        return this.tableLabelName;
    }

    public List<DictionaryMapColumn> fetchDictionaryMapColumns(IntegrationEnvironment integrationEnvironment){
        if(mdColumns == null || mdColumns.size()==0){
            return null;
        }
        List<DictionaryMapColumn> dictionaryMapColumns = new ArrayList<>(4);
        for(MetaColumn mc : mdColumns){
            //dictionary; 解析 mc.getReferenceData() json
            if("1".equals(mc.getReferenceType())){
                if(mc.getReferenceData().startsWith("{")){
                    Object jsonObject = JSON.parse(mc.getReferenceData());
                    if(jsonObject instanceof JSONObject){
                        for(Map.Entry<String, Object> ent: ((JSONObject)jsonObject).entrySet()){
                            dictionaryMapColumns.add(new DictionaryMapColumn(
                                mc.getPropertyName(),
                                ent.getKey(),
                                StringBaseOpt.castObjectToString(ent.getValue())
                            ));
                        }
                    }
                } else {
                    dictionaryMapColumns.add(new DictionaryMapColumn(
                        mc.getPropertyName(),
                        mc.getPropertyName() + "Desc",
                        mc.getReferenceData()
                    ));
                }
            } else // JSON
                if("2".equals(mc.getReferenceType())){
                    String jsonStr = mc.getReferenceData().trim();
                    String catalogCode = Md5Encoder.encodeBase64(jsonStr, true);
                    boolean hasDictoinary = CodeRepositoryUtil.hasExtendedDictionary(catalogCode);
                    if(!hasDictoinary){
                        Object jsonObject = JSON.parse(jsonStr);
                        if(jsonObject instanceof Map) {
                            CodeRepositoryUtil.registeExtendedCodeRepo(catalogCode,
                                CollectionsOpt.objectMapToStringMap((Map)jsonObject));
                            hasDictoinary = true;
                        }
                    }
                    if(hasDictoinary){
                        dictionaryMapColumns.add(new DictionaryMapColumn(
                            mc.getPropertyName(),
                            mc.getPropertyName()+"Desc",
                            catalogCode));
                    }
            } else // sql语句
                if("3".equals(mc.getReferenceType())){
                    String sqlStr = mc.getReferenceData().trim();
                    String catalogCode = Md5Encoder.encodeBase64(sqlStr, true);
                    if(!CodeRepositoryUtil.hasExtendedDictionary(catalogCode)){
                        CodeRepositoryUtil.registeExtendedCodeRepo(catalogCode,
                            new CachedObject<>(
                                new SqlDictionaryMapSupplier(
                                    integrationEnvironment.getDatabaseInfo(this.getDatabaseCode()),
                                    sqlStr),
                                ICachedObject.KEEP_FRESH_PERIOD * 3));
                    }
                    dictionaryMapColumns.add(new DictionaryMapColumn(
                        mc.getPropertyName(),
                        mc.getPropertyName()+"Desc",
                        catalogCode));
            }
        }
        return dictionaryMapColumns;
    }

}
