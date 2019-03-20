package com.centit.product.datapacket.controller;

import com.alibaba.fastjson.JSONObject;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.support.database.utils.PageDesc;
import com.centit.product.datapacket.po.DataPacket;
import com.centit.product.datapacket.service.DataPacketService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.HtmlUtils;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Api(value = "数据包", tags = "数据包")
@RestController
@RequestMapping(value = "packet")
public class DataPacketController extends BaseController {

    @Autowired
    private DataPacketService dataResourceService;

    @ApiOperation(value = "新增数据包")
    @PostMapping
    @WrapUpResponseBody
    public void createDataResource(DataPacket dataResource){
        //dataResource.setQuerySql(HtmlUtils.htmlUnescape(dataResource.getQuerySql()));
        dataResourceService.createDataResource(dataResource);
    }

    @ApiOperation(value = "编辑数据包")
    @PutMapping(value = "/{resourceId}")
    @WrapUpResponseBody
    public void updateDataResource(@PathVariable String resourceId, DataPacket dataResource){
        dataResource.setPacketId(resourceId);
        //dataResource.setQuerySql(HtmlUtils.htmlUnescape(dataResource.getQuerySql()));
        dataResourceService.updateDataResource(dataResource);
    }

    @ApiOperation(value = "删除数据包")
    @DeleteMapping(value = "/{resourceId}")
    @WrapUpResponseBody
    public void deleteDataResource(@PathVariable String resourceId){
        dataResourceService.deleteDataResource(resourceId);
    }

    @ApiOperation(value = "查询数据包")
    @GetMapping
    @WrapUpResponseBody
    public PageQueryResult<DataPacket> listDataResource(PageDesc pageDesc){
        List<DataPacket> list = dataResourceService.listDataResource(new HashMap<>(), pageDesc);
        return PageQueryResult.createResult(list, pageDesc);
    }

    @ApiOperation(value = "查询单个数据包")
    @GetMapping(value = "/{resourceId}")
    @WrapUpResponseBody
    public DataPacket getDataResource(@PathVariable String resourceId){
        return dataResourceService.getDataResource(resourceId);
    }

    @ApiOperation(value = "生成表格数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "databaseCode", value = "数据库代码", required = true),
        @ApiImplicitParam(name = "sql", value = "查询SQL", required = true)
    })
    @GetMapping(value = "/table")
    @WrapUpResponseBody
    public JSONObject generateTable(String databaseCode, String sql, HttpServletRequest request){
        Map<String, Object> params = collectRequestParameters(request);
        JSONObject table = new JSONObject();
        //table.put("column", dataResourceService.generateColumn(databaseCode, HtmlUtils.htmlUnescape(sql)));
        table.put("objList", dataResourceService.queryData(databaseCode, HtmlUtils.htmlUnescape(sql), params));
        return table;
    }

    @ApiOperation(value = "生成参数名称列表")
    @ApiImplicitParam(name = "sql", value = "查询SQL", required = true)
    @GetMapping(value = "/param")
    @WrapUpResponseBody
    public Set<String> generateParam(String sql ){
        return dataResourceService.generateParam(sql);
    }

  }
