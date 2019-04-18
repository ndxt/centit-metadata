package com.centit.product.metadata.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.centit.framework.common.ResponseData;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.product.metadata.service.MetaObjectService;
import com.centit.support.database.utils.PageDesc;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * 对数据库进行简单的增删改查，这个接口不能对外公开
 *
 * 对外公开的应该式自定义数据库表对应的额接口
 */
@RestController
@RequestMapping(value = "chart")
@Api(value = "基于元数据的数据访问服务", tags = "数据访问")
public class MetaObjectController extends BaseController {

    @Autowired
    private MetaObjectService metaObjectService;

    @ApiOperation(value = "分页查询数据库表数据列表")
    @RequestMapping(value = "/{tableId}/list", method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult<Object> listObjects(@PathVariable String tableId, PageDesc pageDesc,
                                               String [] fields,HttpServletRequest request) {
        Map<String, Object> params = collectRequestParameters(request);//convertSearchColumn(request);
        JSONArray ja = metaObjectService.pageQueryObjects(
            tableId, params, pageDesc);
        return PageQueryResult.createJSONArrayResult(ja, pageDesc, fields);
    }

    @ApiOperation(value = "获取一个数据，主键作为参数以key-value形式提交")
    @RequestMapping(value = "/{tableId}", method = RequestMethod.GET)
    @WrapUpResponseBody
    public Map<String, Object> getObject(@PathVariable String tableId,
                                         HttpServletRequest request) {
        Map<String, Object> parameters = collectRequestParameters(request);
        return metaObjectService.getObjectById(tableId, parameters);
    }

    @ApiOperation(value = "修改数据库表数据")
    @RequestMapping(value = "/{tableId}", method = RequestMethod.PUT)
    @WrapUpResponseBody
    public ResponseData updateObject(@PathVariable String tableId,
                                     @RequestBody String jsonString) {
        metaObjectService.updateObject(tableId, JSON.parseObject(jsonString));
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "新增数据库表数据")
    @RequestMapping(value = "/{tableId}", method = RequestMethod.POST)
    @WrapUpResponseBody
    public ResponseData saveObject(@PathVariable String tableId,
                                   @RequestBody String jsonString) {
        metaObjectService.saveObject(tableId, JSON.parseObject(jsonString));
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "删除数据库表数据")
    @RequestMapping(value = "/{tableId}", method = RequestMethod.DELETE)
    @WrapUpResponseBody
    public ResponseData deleteObject(@PathVariable String tableId,
                                     HttpServletRequest request) {
        Map<String, Object> parameters = collectRequestParameters(request);
        metaObjectService.deleteObject(tableId, parameters);
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "获取一个数据带子表，主键作为参数以key-value形式提交")
    @RequestMapping(value = "/{tableId}/withChildren", method = RequestMethod.GET)
    @WrapUpResponseBody
    public Map<String, Object> getObjectWithChildren(@PathVariable String tableId,
                                                     HttpServletRequest request) {
        Map<String, Object> parameters = collectRequestParameters(request);
        return metaObjectService.getObjectWithChildren(tableId, parameters, 1);
    }

    @ApiOperation(value = "修改数据库表数据带子表")
    @RequestMapping(value = "/{tableId}/withChildren", method = RequestMethod.PUT)
    @WrapUpResponseBody
    public ResponseData updateObjectWithChildren(@PathVariable String tableId,
                                                 @RequestBody String jsonString) {
        metaObjectService.mergeObjectWithChildren(tableId, JSON.parseObject(jsonString));
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "新增数据库表数据带子表")
    @RequestMapping(value = "/{tableId}/withChildren", method = RequestMethod.POST)
    @WrapUpResponseBody
    public ResponseData saveObjectWithChildren(@PathVariable String tableId,
                                               @RequestBody String jsonString) {
        metaObjectService.saveObjectWithChildren(tableId, JSON.parseObject(jsonString));
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "删除数据库表数据带子表")
    @RequestMapping(value = "/{tableId}/withChildren", method = RequestMethod.DELETE)
    @WrapUpResponseBody
    public ResponseData deleteObjectWithChildren(@PathVariable String tableId,
                                                 HttpServletRequest request) {
        Map<String, Object> parameters = collectRequestParameters(request);
        metaObjectService.deleteObjectWithChildren(tableId, parameters);
        return ResponseData.makeSuccessResponse();
    }
}
