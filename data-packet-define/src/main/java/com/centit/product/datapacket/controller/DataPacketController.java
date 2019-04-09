package com.centit.product.datapacket.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.centit.framework.common.ObjectException;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.framework.ip.service.IntegrationEnvironment;
import com.centit.framework.security.model.CentitUserDetails;
import com.centit.product.dataopt.bizopt.BuiltInOperation;
import com.centit.product.dataopt.core.BizModel;
import com.centit.product.dataopt.core.DataSet;
import com.centit.product.dataopt.core.SimpleBizModel;
import com.centit.product.dataopt.core.SimpleDataSet;
import com.centit.product.dataopt.dataset.SQLDataSetReader;
import com.centit.product.datapacket.po.DataPacket;
import com.centit.product.datapacket.po.RmdbQuery;
import com.centit.product.datapacket.service.DBPacketBizSupplier;
import com.centit.product.datapacket.service.DataPacketService;
import com.centit.product.datapacket.service.RmdbQueryService;
import com.centit.product.datapacket.utils.DataPacketUtil;
import com.centit.product.datapacket.vo.DataPacketSchema;
import com.centit.support.database.utils.JdbcConnect;
import com.centit.support.database.utils.PageDesc;
import com.centit.support.security.AESSecurityUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api(value = "数据包", tags = "数据包")
@RestController
@RequestMapping(value = "packet")
public class DataPacketController extends BaseController {

    @Autowired
    private JedisPool jedisPool;

    @Autowired
    private DataPacketService dataPacketService;

    @Autowired
    private RmdbQueryService rmdbQueryService;

    @Autowired
    private IntegrationEnvironment integrationEnvironment;

    @ApiOperation(value = "新增数据包")
    @PostMapping
    @WrapUpResponseBody
    public void createDataPacket(DataPacket dataPacket, HttpServletRequest request){
        CentitUserDetails userDetails = WebOptUtils.getLoginUser(request);
        if(userDetails == null){
            throw new ObjectException("未登录");
        }
        dataPacket.setRecorder(userDetails.getUserCode());
        dataPacket.setDataOptDescJson(StringEscapeUtils.unescapeHtml4(dataPacket.getDataOptDescJson()));
        dataPacketService.createDataPacket(dataPacket);
    }

    @ApiOperation(value = "编辑数据包")
    @PutMapping(value = "/{packetId}")
    @WrapUpResponseBody
    public void updateDataPacket(@PathVariable String packetId, @RequestBody DataPacket dataPacket){
        dataPacket.setPacketId(packetId);
        dataPacket.setDataOptDescJson(StringEscapeUtils.unescapeHtml4(dataPacket.getDataOptDescJson()));
        dataPacketService.updateDataPacket(dataPacket);
    }

    @ApiOperation(value = "删除数据包")
    @DeleteMapping(value = "/{packetId}")
    @WrapUpResponseBody
    public void deleteDataPacket(@PathVariable String packetId){
        dataPacketService.deleteDataPacket(packetId);
    }

    @ApiOperation(value = "获取数据包初始模式（不包括数据预处理）")
    @GetMapping(value = "/originschema/{packetId}")
    @WrapUpResponseBody
    public DataPacketSchema getDataPacketOriginSchema(@PathVariable String packetId){
        return DataPacketSchema.valueOf(dataPacketService.getDataPacket(packetId));
    }

    @ApiOperation(value = "获取数据包模式")
    @GetMapping(value = "/schema/{packetId}")
    @WrapUpResponseBody
    public DataPacketSchema getDataPacketSchema(@PathVariable String packetId){
        DataPacket dataPacket = dataPacketService.getDataPacket(packetId);
        DataPacketSchema schema = DataPacketSchema.valueOf(dataPacket);
        JSONObject obj = dataPacket.getDataOptDesc();
        if(obj!=null){
            return DataPacketUtil.calcDataPacketSchema(schema, obj);
        }
        return schema;
    }

    @ApiOperation(value = "查询数据包")
    @GetMapping
    @WrapUpResponseBody
    public PageQueryResult<DataPacket> listDataPacket(PageDesc pageDesc){
        List<DataPacket> list = dataPacketService.listDataPacket(new HashMap<>(), pageDesc);
        return PageQueryResult.createResult(list, pageDesc);
    }

    @ApiOperation(value = "查询单个数据包")
    @GetMapping(value = "/{packetId}")
    @WrapUpResponseBody
    public DataPacket getDataPacket(@PathVariable String packetId){
        return dataPacketService.getDataPacket(packetId);
    }

    private BizModel innerFetchDataPacketData(DataPacket dataPacket, String params){
        DBPacketBizSupplier bizSupplier = new DBPacketBizSupplier(dataPacket);
        bizSupplier.setIntegrationEnvironment(integrationEnvironment);
        if(StringUtils.isNotBlank(params)){
            JSONObject obj = JSON.parseObject(params);
            if(obj!=null){
                bizSupplier.setQueryParams(obj);
            }
        }
        return bizSupplier.get();
    }

    @ApiOperation(value = "获取数据包数据")
    @ApiImplicitParams({@ApiImplicitParam(
        name = "packetId", value="数据包ID",
        required=true, paramType = "path", dataType ="String"
    ), @ApiImplicitParam(
        name = "params", value="查询参数，map的json格式字符串"
    ), @ApiImplicitParam(
        name = "datasets", value="需要返回的数据集名称，用逗号隔开，如果为空返回全部"
    )})
    @GetMapping(value = "/packet/{packetId}")
    @WrapUpResponseBody
    public BizModel fetchDataPacketData(@PathVariable String packetId, String params, String datasets){
        DataPacket dataPacket = dataPacketService.getDataPacket(packetId);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        String dateString = formatter.format(dataPacket.getRecordDate());
        StringBuffer temp = new StringBuffer("packet:");
        temp.append(packetId)
            .append(":")
            .append(dataPacket.getBufferFreshPeriod())
            .append(dateString);
        String key = AESSecurityUtils.encryptAndBase64(temp.toString(), DatabaseInfo.DESKEY) ;
        Object object = null;
        if (dataPacket.getBufferFreshPeriod() >=0 ) {
            Jedis jedis = jedisPool.getResource();
            //jedis.get(key);
            if (jedis.get(key.getBytes())!=null && !"".equals(jedis.get(key.getBytes()))) {
                try {
                    byte[] byt = jedis.get(key.getBytes());
                    ObjectInputStream ois = null;
                    ByteArrayInputStream bis = null;
                    bis = new ByteArrayInputStream(byt);
                    ois = new ObjectInputStream(bis);
                    object = ois.readObject();
                    bis.close();
                    ois.close();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
                jedis.close();
                if (object!=null) {
                    BizModel bizModel = (BizModel) object;
                    return bizModel;
                }
            }
        }
        BizModel bizModel = innerFetchDataPacketData(dataPacket, params);
        JSONObject obj = dataPacket.getDataOptDesc();
        Jedis jedis = jedisPool.getResource();
        if (jedis.get(key.getBytes())==null || "".equals(jedis.get(key.getBytes()))) {
            //jedis.set(key,bizModel.toString());
            try {
                ObjectOutputStream oos = null;
                ByteArrayOutputStream bos = null;
                bos = new ByteArrayOutputStream();
                oos = new ObjectOutputStream(bos);
                if(obj!=null) {
                    BuiltInOperation builtInOperation = new BuiltInOperation(obj);
                    oos.writeObject(builtInOperation.apply(bizModel));
                } else {
                    oos.writeObject(bizModel);
                }
                byte[] byt=bos.toByteArray();
                jedis.set(key.getBytes(),byt);
                bos.close();
                oos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        jedis.close();
        if(obj!=null){
            BuiltInOperation builtInOperation = new BuiltInOperation(obj);
            bizModel = builtInOperation.apply(bizModel);
        }

        if(StringUtils.isNotBlank(datasets)){
            String[] dss = datasets.split(",");
            SimpleBizModel dup = new SimpleBizModel(bizModel.getModelName());
            dup.setModelTag(bizModel.getModelTag());
            Map<String, DataSet> dataMap = new HashMap<>(dss.length+1);
            for(String dsn : dss) {
                DataSet ds = bizModel.fetchDataSetByName(dsn);
                if(ds!=null){
                    dataMap.put(dsn, ds);
                }
            }
            dup.setBizData(dataMap);
            return dup;
        }
        return bizModel;
    }

    @ApiOperation(value = "获取数据库查询数据")
    @ApiImplicitParams({@ApiImplicitParam(
        name = "queryId", value="数据查询ID",
        required=true, paramType = "path", dataType ="String"
    ), @ApiImplicitParam(
        name = "params", value="查询参数，map的json格式字符串"
    )})
    @GetMapping(value = "/dbquery/{queryId}")
    @WrapUpResponseBody
    public SimpleDataSet fetchDBQueryData(@PathVariable String queryId, String params){
        RmdbQuery query = rmdbQueryService.getDbQuery(queryId);
        DataPacket dataPacket = dataPacketService.getDataPacket(query.getPacketId());
        Map<String, Object> modelTag = dataPacket.getPacketParamsValue();

        SQLDataSetReader sqlDSR = new SQLDataSetReader();
        sqlDSR.setDataSource(JdbcConnect.mapDataSource(
            integrationEnvironment.getDatabaseInfo(query.getDatabaseCode())));
        sqlDSR.setSqlSen(query.getQuerySQL());

        if(StringUtils.isNotBlank(params)){
            JSONObject obj = JSON.parseObject(params);
            if(obj!=null){
                modelTag.putAll(obj);
            }
        }
        return sqlDSR.load(modelTag);
    }

    @ApiOperation(value = "获取数据包数据并对数据进行业务处理")
    @ApiImplicitParams({@ApiImplicitParam(
        name = "queryId", value="数据查询ID",
        required=true, paramType = "path", dataType ="String"
    ), @ApiImplicitParam(
        name = "optsteps", value="数据操作，steps的json格式字符串，参见js代码中的说明"
    ), @ApiImplicitParam(
        name = "params", value="查询参数，map的json格式字符串"
    )})
    @GetMapping(value = "/dataopts/{packetId}")
    @WrapUpResponseBody
    public BizModel fetchDataPacketDataWithOpt(@PathVariable String packetId, String optsteps, String params){
        DataPacket dataPacket = dataPacketService.getDataPacket(packetId);
        BizModel bizModel = innerFetchDataPacketData(dataPacket, params);
        if(StringUtils.isNotBlank(optsteps)){
            JSONObject obj = JSON.parseObject(optsteps);
            if(obj!=null){
                BuiltInOperation builtInOperation = new BuiltInOperation(obj);
                return builtInOperation.apply(bizModel);
            }
        }
        return bizModel;
    }
}
