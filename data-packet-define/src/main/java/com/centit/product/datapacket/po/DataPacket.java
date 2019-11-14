package com.centit.product.datapacket.po;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.centit.framework.core.dao.DictionaryMap;
import com.centit.support.database.orm.GeneratorCondition;
import com.centit.support.database.orm.GeneratorTime;
import com.centit.support.database.orm.GeneratorType;
import com.centit.support.database.orm.ValueGenerator;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;
import java.io.Serializable;
import java.util.*;

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
    @ValueGenerator(strategy = GeneratorType.UUID)
    private String packetId;

    @ApiModelProperty(value = "数据包名称模板")
    @Column(name = "PACKET_NAME")
    @NotBlank(message = "字段不能为空")
    private String packetName;

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

    @Column(name = "HAS_DATA_OPT")
    @ApiModelProperty(value = "是否有数据预处理", required = true)
    private String hasDataOpt;

    @JSONField(serialize=false)
    @Column(name = "DATA_OPT_DESC_JSON")
    @ApiModelProperty(value = "数据预处理描述 json格式的数据预处理说明", required = true)
    private String dataOptDescJson;

    @Column(name = "BUFFER_FRESH_PERIOD")
    @ApiModelProperty(value = "数据缓存有效期，-1：不缓存（默认值） 0 永不失效 1 一日，2 按周（注意不是一周） 3 按月 4 按年， >=60 代表时间单位为秒", required = true)
    private Integer bufferFreshPeriod;

    @Column(name = "RECORDER")
    @ApiModelProperty(value = "修改人", hidden = true)
    @DictionaryMap(fieldName = "recorderName", value = "userCode")
    private String recorder;

    @Column(name = "RECORD_DATE")
    @ApiModelProperty(value = "修改时间", hidden = true)
    @ValueGenerator(strategy = GeneratorType.FUNCTION, occasion = GeneratorTime.NEW_UPDATE, condition = GeneratorCondition.ALWAYS, value = "today()")
    @JSONField(serialize = false)
    private Date recordDate;

    @ApiModelProperty(value = "业务模块代码")
    @Column(name = "APPLICATION_ID")
    private String  applicationId;

    @OneToMany(targetEntity = DataPacketParam.class)
    @JoinColumn(name = "packetId", referencedColumnName = "packetId")
    private List<DataPacketParam> packetParams;

    @OneToMany(targetEntity = DataSetDefine.class)
    @JoinColumn(name = "packetId", referencedColumnName = "packetId")
    private List<DataSetDefine> dataSetDefines;

    public DataPacket(){
        bufferFreshPeriod = -1;
    }

    public List<DataSetDefine> getDataSetDefines() {
            return dataSetDefines;
    }

    public List<DataPacketParam> getPacketParams() {
        if(packetParams==null){
            packetParams = new ArrayList<>(2);
        }
        return packetParams;
    }

    @JSONField(serialize = false)
    public Map<String, Object> getPacketParamsValue(){
        Map<String, Object> params = new HashMap<>();
        if(packetParams == null){
            return params;
        }
        for(DataPacketParam packetParam : packetParams){
            params.put(packetParam.getParamName(), packetParam.getParamDefaultValue());
        }
        return params;
    }

    public JSONObject getDataOptDesc() {
        if(StringUtils.isBlank(dataOptDescJson)) {
            return null;
        }
        try {
            return JSONObject.parseObject(dataOptDescJson);
        } catch (Exception e) {
            e.printStackTrace();
            return  null;
        }

    }

}
