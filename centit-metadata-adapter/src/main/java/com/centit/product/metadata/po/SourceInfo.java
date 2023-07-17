package com.centit.product.metadata.po;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.annotation.JSONField;
import com.centit.framework.core.dao.DictionaryMap;
import com.centit.product.metadata.api.ISourceInfo;
import com.centit.support.database.orm.GeneratorCondition;
import com.centit.support.database.orm.GeneratorTime;
import com.centit.support.database.orm.GeneratorType;
import com.centit.support.database.orm.ValueGenerator;
import com.centit.support.security.SecurityOptUtils;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.Length;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

/**
 * @author zhf
 */
@Data
@Entity
@Table(name = "F_DATABASE_INFO")
@ApiModel(value = "集成资源库信息对象", description = "集成资源库信息对象 DatabaseInfo")
public class SourceInfo implements ISourceInfo, Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "DATABASE_CODE")
    @ApiModelProperty(value = "数据库代码", name = "databaseCode")
    @ValueGenerator(strategy = GeneratorType.UUID22)
    private String databaseCode;

    @Column(name = "OS_ID")
    @ApiModelProperty(value = "系统代码", name = "osId")
    private String osId;

    @Column(name = "top_unit")
    @ApiModelProperty(value = "所属租户", name = "topUnit")
    private String topUnit;


    @Column(name = "DATABASE_NAME")
    @Length(max = 100, message = "字段长度不能大于{max}")
    @ApiModelProperty(value = "数据库名", name = "databaseName")
    private String databaseName;

    @Column(name = "DATABASE_URL")
    @Length(max = 1000, message = "字段长度不能大于{max}")
    @ApiModelProperty(value = "数据库地址", name = "databaseUrl")
    private String databaseUrl;

    @Column(name = "USERNAME")
    @Length(max = 100, message = "字段长度不能大于{max}")
    @ApiModelProperty(value = "数据库用户名", name = "username")
    private String username;

    @Column(name = "PASSWORD")
    @Length(max = 100, message = "字段长度不能大于{max}")
    @ApiModelProperty(value = "数据库密码", name = "password")
    private String password;

    @Column(name = "DATABASE_DESC")
    @Length(max = 500, message = "字段长度不能大于{max}")
    @ApiModelProperty(value = "数据库描述信息", name = "databaseDesc")
    private String databaseDesc;

    @ApiModelProperty(value = "修改时间", name = "lastModifyDate")
    @ValueGenerator(strategy = GeneratorType.FUNCTION, value = "today()", condition = GeneratorCondition.ALWAYS, occasion = GeneratorTime.ALWAYS)
    @Column(name = "LAST_MODIFY_DATE")
    private Date lastModifyDate;

    @ApiModelProperty(value = "创建人", name = "CREATED")
    @Column(name = "CREATED")
    @Length(max = 32, message = "字段长度不能大于{max}")
    @DictionaryMap(fieldName = "createUserName", value = "userCode")
    private String created;

    @ApiModelProperty(value = "创建时间", name = "CREATE_TIME")
    @ValueGenerator(strategy = GeneratorType.FUNCTION, value = "today()")
    @Column(name = "CREATE_TIME")
    private Date createTime;

    @ApiModelProperty(value = "扩展属性，json格式，clob字段")
    @Column(name = "EXT_PROPS")
    @Basic(fetch = FetchType.LAZY)
    private JSONObject extProps;

    @ApiModelProperty(value = "资源类型,D:关系数据库 M:MongoDb R:redis E:elssearch K:kafka B:rabbitmq,H http服务", name = "SOURCE_TYPE")
    @Column(name = "SOURCE_TYPE")
    private String sourceType;
    // Constructors

    /**
     * default constructor
     */
    public SourceInfo() {
    }

    public SourceInfo(String databaseCode, String databaseName) {
        this.databaseCode = databaseCode;
        this.databaseName = databaseName;
    }

    public SourceInfo(String databaseCode, String databaseName, String databaseUrl,
                      String username, String password, String dataDesc) {
        this.databaseCode = databaseCode;
        this.databaseName = databaseName;
        this.databaseUrl = databaseUrl;
        this.username = username;
        this.password = password;
        this.databaseDesc = dataDesc;
    }

    public void setPassword(String pwd) {
        if (StringUtils.isNotBlank(pwd)) {
            if (pwd.startsWith("cipher:")) {
                this.password = pwd;
            } else {
                this.password = SecurityOptUtils.encodeSecurityString(pwd, "cipher");
            }
        }
    }

    @Override
    @JSONField(serialize = false)
    public String getClearPassword() {
        return SecurityOptUtils.decodeSecurityString(this.password);
    }

    @Override
    public boolean equals(Object dbco) {
        if (this == dbco) {
            return true;
        }

        if (dbco instanceof ISourceInfo) {
            ISourceInfo dbc = (ISourceInfo) dbco;
            return databaseUrl != null && databaseUrl.equals(dbc.getDatabaseUrl())
                && databaseCode != null && databaseCode.equals(dbc.getDatabaseCode())
                && ((username==null && dbc.getUsername()==null)
                  || (username != null && username.equals(dbc.getUsername())))
                && ((password==null && dbc.getPassword()==null)
                || (password != null && password.equals(dbc.getPassword())))
                && ((extProps==null && dbc.getExtProps()==null)
                || (extProps != null && extProps.equals(dbc.getExtProps())));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result +
            (this.databaseUrl == null ? 0 : this.databaseUrl.hashCode());
        result = 37 * result +
            (this.databaseCode == null ? 0 : this.databaseCode.hashCode());
        result = 37 * result +
            (this.username == null ? 0 : this.getUsername().hashCode());
        result = 37 * result +
            (this.password == null ? 0 : this.password.hashCode());
        result = 37 * result +
            (this.extProps == null ? 0 : this.extProps.hashCode());
        return result;
    }
}
