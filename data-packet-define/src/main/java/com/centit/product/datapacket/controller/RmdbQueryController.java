package com.centit.product.datapacket.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.product.datapacket.po.RmdbQuery;
import com.centit.product.datapacket.service.RmdbQueryService;
import com.centit.product.datapacket.vo.ColumnSchema;
import com.centit.support.database.utils.PageDesc;
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

@Api(value = "数据库查询", tags = "数据库查询")
@RestController
@RequestMapping(value = "query")
public class RmdbQueryController extends BaseController {

    @Autowired
    private RmdbQueryService rmdbQueryService;

    @ApiOperation(value = "新增数据库查询")
    @PostMapping
    @WrapUpResponseBody
    public void createDbQuery(RmdbQuery rmdbQuery){
        rmdbQueryService.createDbQuery(rmdbQuery);
    }

    @ApiOperation(value = "编辑数据库查询")
    @PutMapping(value = "/{queryId}")
    @WrapUpResponseBody
    public void updateDbQuery(@PathVariable String queryId, RmdbQuery rmdbQuery){
        rmdbQuery.setQueryId(queryId);
        //rmdbQuery.setQuerySql(HtmlUtils.htmlUnescape(dataResource.getQuerySql()));
        rmdbQueryService.updateDbQuery(rmdbQuery);
    }

    @ApiOperation(value = "删除数据库查询")
    @DeleteMapping(value = "/{queryId}")
    @WrapUpResponseBody
    public void deleteDbQuery(@PathVariable String queryId){
        rmdbQueryService.deleteDbQuery(queryId);
    }

    @ApiOperation(value = "查询数据库查询")
    @GetMapping
    @WrapUpResponseBody
    public PageQueryResult<RmdbQuery> listDbQuery(PageDesc pageDesc){
        List<RmdbQuery> list = rmdbQueryService.listDbQuery(new HashMap<>(), pageDesc);
        return PageQueryResult.createResult(list, pageDesc);
    }

    @ApiOperation(value = "查询单个数据库查询")
    @GetMapping(value = "/{queryId}")
    @WrapUpResponseBody
    public RmdbQuery getDbQuery(@PathVariable String queryId){
        return rmdbQueryService.getDbQuery(queryId);
    }

    @ApiOperation(value = "预览数据值返回前20行")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "databaseCode", value = "数据库代码", required = true),
        @ApiImplicitParam(name = "sql", value = "查询SQL", required = true)
    })
    @GetMapping(value = "/reviewdata")
    @WrapUpResponseBody
    public JSONArray generateTable(String databaseCode, String sql, HttpServletRequest request){
        Map<String, Object> params = collectRequestParameters(request);
        //table.put("column", rmdbQueryService.generateColumn(databaseCode, HtmlUtils.htmlUnescape(sql)));
        return rmdbQueryService.queryViewSqlData(databaseCode, HtmlUtils.htmlUnescape(sql), params);
    }

    @ApiOperation(value = "生成查询字段列表")
    @ApiImplicitParam(name = "sql", value = "查询SQL", required = true)
    @GetMapping(value = "/sqlcolumn")
    @WrapUpResponseBody
    public List<ColumnSchema> generateSqlcolumn(String databaseCode, String sql, HttpServletRequest request){
        Map<String, Object> params = collectRequestParameters(request);
        return rmdbQueryService.generateSqlFields(databaseCode, HtmlUtils.htmlUnescape(sql), params);
    }


    @ApiOperation(value = "生成参数名称列表")
    @ApiImplicitParam(name = "sql", value = "查询SQL", required = true)
    @GetMapping(value = "/param")
    @WrapUpResponseBody
    public Set<String> generateParam(String sql ){
        return rmdbQueryService.generateSqlParams(sql);
    }

}
