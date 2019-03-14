package com.centit.support.datapacket.controller;

import com.centit.framework.core.controller.BaseController;
import com.centit.support.datapacket.service.RmdbQueryService;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(value = "数据包", tags = "数据包")
@RestController
@RequestMapping(value = "data_resource")
public class RmdbQueryController extends BaseController {

    @Autowired
    private RmdbQueryService rmdbQueryService;

    /*@ApiOperation(value = "新增数据包")
    @PostMapping
    @WrapUpResponseBody
    public void createDataResource(DataPacket dataResource){
        dataResource.setQuerySql(HtmlUtils.htmlUnescape(dataResource.getQuerySql()));
        dataResourceService.createDataResource(dataResource);
    }

    @ApiOperation(value = "编辑数据包")
    @PutMapping(value = "/{resourceId}")
    @WrapUpResponseBody
    public void updateDataResource(@PathVariable String resourceId, DataPacket dataResource){
        dataResource.setPacketId(resourceId);
        dataResource.setQuerySql(HtmlUtils.htmlUnescape(dataResource.getQuerySql()));
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
        table.put("column", dataResourceService.generateColumn(databaseCode, HtmlUtils.htmlUnescape(sql)));
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

    @ApiOperation(value = "统计数据")
    @ApiImplicitParam(name = "resourceId", value = "数据包ID", required = true)
    @GetMapping(value = "/stat/{resourceId}")
    @WrapUpResponseBody
    public JSONObject stat(HttpServletRequest request, @PathVariable String resourceId){
        JSONObject table = new JSONObject();
        DataPacket resource = dataResourceService.getDataResource(resourceId);
        Map<String, Object> params = collectRequestParameters(request);
        if(resource.getParams() != null) {
            for (DataResourceParam param : resource.getParams()) {
                if (!params.containsKey(param.getParamName())) {
                    params.put(param.getParamName(), param.getParamDefaultValue());
                }
            }
        }
        table.put("column", resource.getColumns());
        table.put("param", resource.getParams());
        table.put("objList", dataResourceService.queryData(resource.getDatabaseCode(), HtmlUtils.htmlUnescape(resource.getQuerySql()), params));
        return table;
    }

    @ApiOperation(value = "修改数据包列")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "resourceId", value = "数据包ID", required = true),
        @ApiImplicitParam(name = "columnCode", value = "列代码", required = true)
    })
    @PutMapping(value = "/{resourceId}/{columnCode}")
    @WrapUpResponseBody
    public void updateResourceColumn(@PathVariable String resourceId, @PathVariable String columnCode, DataResourceColumn column){
        column.setResourceId(resourceId);
        column.setColumnCode(columnCode);
        dataResourceService.updateResourceColumn(column);
    }
*/
  }
