package com.centit.product.datapacket.po;

import com.alibaba.fastjson.annotation.JSONField;
import com.centit.framework.core.dao.DictionaryMap;
import com.centit.support.database.orm.GeneratorCondition;
import com.centit.support.database.orm.GeneratorTime;
import com.centit.support.database.orm.GeneratorType;
import com.centit.support.database.orm.ValueGenerator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import java.io.Serializable;
import java.util.Date;
import java.util.List;

@ApiModel
@Data
@Entity
@Table(name = "Q_DATA_PACKET")
public class DataPacket implements Serializable {
    private static final long serialVersionUID = 1;

    @ApiModelProperty(value = "数据包ID", hidden = true)
    @Id
    @Column(name = "PACKET_ID")
    @NotBlank(message = "字段不能为空")
    private String packetId;

    @ApiModelProperty(value = "数据包名称模板")
    @Column(name = "PACKET_NAME")
    @NotBlank(message = "字段不能为空")
    private String packetName;
    /**
     * 数据包类别，主要有 D database， F file ， P directory 文件夹 , 默认值为D
     */
    @ApiModelProperty(value = "数据包类别，主要有 D database, F file, P directory 文件夹, 默认值为D")
    @Column(name = "PACKET_TYPE")
    @NotBlank(message = "字段不能为空")
    private String packetType;

    /*
     * 数据包参数： 查询参数描述
     */
    /*@ApiModelProperty(value = "数据包参数： 查询参数描述")
    @Column(name = "PACKET_PARAMS_JSON")
    private String packetParamsJSON;*/
    /**
     * 详细描述
     */
    @ApiModelProperty(value = "详细描述")
    @Column(name = "PACKET_DESC")
    private String packetDesc;

    @Column(name = "OWNER_TYPE")
    @ApiModelProperty(value = "属主类别（D:部门；U:用户）")
    private String ownerType;

    @Column(name = "OWNER_CODE")
    @ApiModelProperty(value = "属主代码")
    private String ownerCode;

    @Column(name = "RECORDER")
    @ApiModelProperty(value = "修改人", hidden = true)
    @DictionaryMap(fieldName = "recorderName", value = "userCode")
    private String recorder;

    @Column(name = "RECORD_DATE")
    @ApiModelProperty(value = "修改时间", hidden = true)
    @ValueGenerator(strategy = GeneratorType.FUNCTION, occasion = GeneratorTime.NEW_UPDATE, condition = GeneratorCondition.ALWAYS, value = "today()")
    @JSONField(serialize = false)
    private Date recordDate;


    @OneToMany(targetEntity = DataPacketParam.class)
    @JoinColumn(name = "packetId", referencedColumnName = "packetId")
    private List<DataPacketParam> packetParams;

    @OneToMany(targetEntity = RmdbQuery.class)
    @JoinColumn(name = "packetId", referencedColumnName = "packetId")
    private List<DataPacketParam> rmdbQueries;

/*    @Transient
    private Map<String, Object> packetParams;*/

    public DataPacket(){
        packetType = "D";
    }

    public List<RmdbQuery> getDBQueries(){
        if("D".equals(packetType)){

        }
        return null;
    }

    /*public Map<String, Object> getPacketParams(){
        if(packetParams == null && StringUtils.isNotBlank(packetParamsJSON)){
            packetParams = JSONObject.parseObject(packetParamsJSON);
        }
        return packetParams;
    }

    public void setPacketParamsJSON(String packetParamsJSON) {
        this.packetParamsJSON = packetParamsJSON;
        this.packetParams = null;
    }*/
}
