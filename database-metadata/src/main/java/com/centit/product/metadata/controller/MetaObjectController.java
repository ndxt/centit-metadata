package com.centit.product.metadata.controller;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.framework.common.ResponseData;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.components.CodeRepositoryUtil;
import com.centit.framework.components.OperationLogCenter;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.framework.model.adapter.PlatformEnvironment;
import com.centit.framework.model.basedata.OperationLog;
import com.centit.framework.model.basedata.OsInfo;
import com.centit.framework.model.basedata.WorkGroup;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaDataCache;
import com.centit.product.metadata.service.MetaObjectService;
import com.centit.product.metadata.transaction.MetadataJdbcTransaction;
import com.centit.product.metadata.utils.SessionDataUtils;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import com.centit.support.security.SecurityOptUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对数据库进行简单的增删改查，这个接口不能对外公开
 * <p>
 * 对外公开的应该式自定义数据库表对应的额接口
 */
@RestController
@RequestMapping(value = "object")
@Api(value = "基于元数据的数据访问服务", tags = "数据访问")
public class MetaObjectController extends BaseController {

    @Autowired
    private MetaObjectService metaObjectService;

    @Autowired
    private MetaDataCache metaDataCache;

    @Autowired
    private PlatformEnvironment platformEnvironment;

    @ApiOperation(value = "分页查询数据库表数据列表")
    @RequestMapping(value = "/{tableId}/list", method = RequestMethod.GET)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public PageQueryResult<Object> listObjects(@PathVariable String tableId, PageDesc pageDesc,
                                               String[] fields, HttpServletRequest request) {
        checkUserOptPower(request,true);
        Map<String, Object> params = collectRequestParameters(request);
        JSONArray ja = metaObjectService.pageQueryObjects(
            tableId, params, pageDesc);
        return PageQueryResult.createJSONArrayResult(ja, pageDesc, fields);
    }

    @ApiOperation(value = "获取一个数据，主键作为参数以key-value形式提交")
    @RequestMapping(value = "/{tableId}", method = RequestMethod.GET)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public Map<String, Object> getObject(@PathVariable String tableId,
                                         HttpServletRequest request) {
        checkUserOptPower(request,true);
        Map<String, Object> parameters = collectRequestParameters(request);
        return metaObjectService.getObjectById(tableId, parameters);
    }

    @ApiOperation(value = "修改数据库表数据")
    @RequestMapping(value = "/{tableId}", method = RequestMethod.PUT)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData updateObject(@PathVariable String tableId,
                                     @RequestBody String jsonString, HttpServletRequest request) {
        checkUserOptPower(request,true);
        metaObjectService.updateObject(tableId, JSON.parseObject(jsonString));
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "根据属性修改数据库表数据")
    @RequestMapping(value = "/{tableId}/batch", method = RequestMethod.PUT)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData batchUpdateObject(@PathVariable String tableId,
                                          @RequestBody String fieldsObject, HttpServletRequest request) {
        checkUserOptPower(request,true);
        Map<String, Object> params = collectRequestParameters(request);
        JSONObject object = JSON.parseObject(fieldsObject);
        int iReturn = metaObjectService.updateObjectsByProperties(tableId, object, params);
        if (iReturn == 0) {
            return ResponseData.makeErrorMessage("无对应sql生成");
        } else {
            return ResponseData.makeSuccessResponse("成功更新" + iReturn + "条");
        }
    }

    @ApiOperation(value = "批量删除数据库表数据")
    @RequestMapping(value = "/{tableId}/batchDelete", method = RequestMethod.PUT)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData batchDeleteObject(@PathVariable String tableId,
                                          @RequestBody String primaryArray, Integer withChildrenDeep,
                                          HttpServletRequest request) {
        checkUserOptPower(request,true);
        JSONArray tempJsonArray = JSON.parseArray(primaryArray);
        tempJsonArray.forEach(object ->
            metaObjectService.deleteObjectWithChildren(tableId, CollectionsOpt.objectToMap(object), withChildrenDeep == null ? 1 : withChildrenDeep));
        return ResponseData.makeSuccessResponse("成功删除" + tempJsonArray.size() + "条");
    }

    @ApiOperation(value = "新增数据库表数据")
    @RequestMapping(value = "/{tableId}", method = RequestMethod.POST)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData saveObject(@PathVariable String tableId,
                                   @RequestBody String jsonString, HttpServletRequest request) {
        checkUserOptPower(request,true);
        metaObjectService.saveObject(tableId, JSON.parseObject(jsonString));
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "删除数据库表数据")
    @RequestMapping(value = "/{tableId}", method = RequestMethod.DELETE)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData deleteObject(@PathVariable String tableId,
                                     HttpServletRequest request) {
        checkUserOptPower(request,true);
        Map<String, Object> parameters = collectRequestParameters(request);
        metaObjectService.deleteObject(tableId, parameters);
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "批量merge数据带子表")
    @RequestMapping(value = "/{tableId}/batchMerge", method = RequestMethod.POST)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public List<Map<String, Object>> batchMergeObjectWithChildren(@PathVariable String tableId,
                                                                  @RequestBody String mergeArray,
                                                                  Integer withChildrenDeep,
                                                                  HttpServletRequest request) {
        HashMap<String, Object> hashMap = SessionDataUtils.createSessionDataMap(
            WebOptUtils.getCurrentUserDetails(request));
        checkUserOptPower(request,true);
        JSONArray jsonArray = JSON.parseArray(mergeArray);
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        List<Map<String, Object>> list = new ArrayList<>();
        jsonArray.forEach(object -> {
            innerMergeObject(tableId, tableInfo, (JSONObject) object, hashMap, withChildrenDeep == null ? 1 : withChildrenDeep);
            Map<String, Object> primaryKey = tableInfo.fetchObjectPk((JSONObject) object);
            list.add(primaryKey);
        });
        return list;
    }

    private void innerMergeObject(String tableId, MetaTable tableInfo, JSONObject object, Map<String, Object> extParams, Integer withChildrenDeep) {
        Map<String, Object> dbObjectPk = tableInfo.fetchObjectPk(object);
        Map<String, Object> dbObject = dbObjectPk == null ? null :
            metaObjectService.getObjectById(tableId, dbObjectPk);
        if (dbObject == null) {
            metaObjectService.saveObjectWithChildren(tableId, object, extParams, withChildrenDeep);
        } else {
            metaObjectService.updateObjectWithChildren(tableId, object, extParams, withChildrenDeep);
        }
    }

    @ApiOperation(value = "获取一个数据带子表，主键作为参数以key-value形式提交")
    @RequestMapping(value = "/{tableId}/withChildren", method = RequestMethod.GET)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public Map<String, Object> getObjectWithChildren(@PathVariable String tableId, Integer withChildrenDeep,
                                                     HttpServletRequest request) {
        checkUserOptPower(request,true);
        Map<String, Object> parameters = collectRequestParameters(request);
        return metaObjectService.getObjectWithChildren(tableId, parameters, withChildrenDeep == null ? 1 : withChildrenDeep);
    }

    @ApiOperation(value = "修改数据库表数据带子表")
    @RequestMapping(value = "/{tableId}/withChildren", method = RequestMethod.PUT)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData updateObjectWithChildren(@PathVariable String tableId, Integer withChildrenDeep,
                                                 @RequestBody String jsonString, HttpServletRequest request) {
        checkUserOptPower(request,true);
        metaObjectService.updateObjectWithChildren(
            tableId, JSON.parseObject(jsonString),
            SessionDataUtils.createSessionDataMap(WebOptUtils.getCurrentUserDetails(request)),
            withChildrenDeep == null ? 1 : withChildrenDeep);
        saveOperationLog(request, jsonString, tableId, "update");
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "新增数据库表数据带子表")
    @RequestMapping(value = "/{tableId}/withChildren", method = RequestMethod.POST)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData saveObjectWithChildren(@PathVariable String tableId, Integer withChildrenDeep,
                                               @RequestBody String jsonString, HttpServletRequest request) {
        checkUserOptPower(request,true);
        metaObjectService.saveObjectWithChildren(tableId, JSON.parseObject(jsonString), withChildrenDeep == null ? 1 : withChildrenDeep);
        saveOperationLog(request, jsonString, tableId, "save");
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "删除数据库表数据带子表")
    @RequestMapping(value = "/{tableId}/withChildren", method = RequestMethod.DELETE)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData deleteObjectWithChildren(@PathVariable String tableId, Integer withChildrenDeep,
                                                 HttpServletRequest request) {
        checkUserOptPower(request,true);
        Map<String, Object> parameters = collectRequestParameters(request);
        metaObjectService.deleteObjectWithChildren(tableId, parameters, withChildrenDeep == null ? 1 : withChildrenDeep);
        saveOperationLog(request, parameters, tableId, "delete");
        return ResponseData.makeSuccessResponse();
    }

    private void saveOperationLog(HttpServletRequest request, Object newValue, String optTag, String optMethod) {
        MetaTable tableInfo = metaDataCache.getTableInfo(optTag);
        if (tableInfo != null && tableInfo.isWriteOptLog()) {
            OperationLogCenter.log(OperationLog.create().user(WebOptUtils.getCurrentUserCode(request))
                    .unit(WebOptUtils.getCurrentUnitCode(request)).topUnit(WebOptUtils.getCurrentTopUnit(request))
                    .correlation(WebOptUtils.getCorrelationId(request)).tag(optTag).method(optMethod)
                .operation("metaData").content("元数据操作").newObject(newValue));
        }
    }

    /**
     * 判断当前人员是否具有操作权限
     * 1.未登录者没有权限
     * 2.登录人没有加入任何租户没有权限
     * 3.加入租户内没有任何应用没有权限
     * 4.登录人没有加入开发组没有权限
     *
     */
    private void checkUserOptPower(HttpServletRequest request, boolean checkInWorkGroup) {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        if (StringUtils.isBlank(userCode)) {
            userCode = WebOptUtils.getRequestFirstOneParameter(request, "userCode");
        }
        if (StringUtils.isBlank(userCode)) {
            throw new ObjectException(ResponseData.ERROR_USER_NOT_LOGIN, "您未登录!");
        }
        String topUnit = WebOptUtils.getCurrentTopUnit(request);
        if (StringUtils.isBlank(topUnit)) {
            throw new ObjectException(ResponseData.ERROR_PRECONDITION_FAILED, "您还未加入任何租户!");
        }

        List<OsInfo> osInfos = CodeRepositoryUtil.listOsInfo(topUnit);
        if (CollectionUtils.sizeIsEmpty(osInfos)) {
            throw new ObjectException(ResponseData.ERROR_PRECONDITION_FAILED, "您当前所在的租户还未创建任何应用!");
        }

        if(checkInWorkGroup) {
            for(OsInfo osInfo : osInfos){
                List<WorkGroup> userGroups = platformEnvironment.listWorkGroup(osInfo.getOsId(), userCode, null);
                if(CollectionUtils.isNotEmpty(userGroups)){
                    return;
                }
            }
            throw new ObjectException(ResponseData.HTTP_NON_AUTHORITATIVE_INFORMATION, "您没有权限！");
        }
    }

    @ApiOperation(value = "根据自定义查询获取数据")
    @RequestMapping(value = "/sqlQuery", method = RequestMethod.PUT)
    @ApiImplicitParam(
        name = "queryJson", value = "查询语句：属性 database 数据库id，sql 查询语句，以aescbc:开头 ，params 参数Map 如有是分页查询必须有参数 pageSize ",
        required = true, paramType = "path", dataType = "String")
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public JSONArray queryDatabaseBySql(@RequestBody String queryJson, HttpServletRequest request) {
        checkUserOptPower(request, false);
        JSONObject json = JSONObject.parseObject(queryJson);
        String sqlSen = json.getString("sql");
        if(StringUtils.isBlank(sqlSen) || !sqlSen.startsWith("aescbc:")) {
            throw new ObjectException(ObjectException.DATA_VALIDATE_ERROR, "查询语句格式不正确!");
        }
        sqlSen = SecurityOptUtils.decodeSecurityString(sqlSen);
        if(!StringUtils.startsWithIgnoreCase(sqlSen, "select") ||
            StringUtils.containsAnyIgnoreCase(sqlSen, ";",
                "update", "delete", "insert", "drop", "create")) {
            throw new ObjectException(ObjectException.DATA_VALIDATE_ERROR, "查询语句格式不正确!");
        }
        String databaseCode = json.getString("database");
        JSONObject params = json.getJSONObject("params");
        PageDesc pageDesc = null;
        Object pageSize = params.get("pageSize");
        if(pageSize != null && NumberBaseOpt.castObjectToInteger(pageSize, -1)>0){
            pageDesc = new PageDesc();
            pageDesc.setPageSize(NumberBaseOpt.castObjectToInteger(pageSize, -1));
            pageDesc.setPageNo(NumberBaseOpt.castObjectToInteger(params.get("pageNo"), 1));
        }
        if(pageDesc!=null){
            return metaObjectService.pageQueryDatas(databaseCode, sqlSen, params, pageDesc);
        } else {
            return metaObjectService.queryDatas(databaseCode, sqlSen, params);
        }
    }

    @ApiOperation(value = "获取表字段的参照引用数据")@ApiImplicitParams({
        @ApiImplicitParam(
            name = "tableId", value = "表id",
            required = true, paramType = "path", dataType = "String"),
        @ApiImplicitParam(
            name = "columnCode", value = "字段代码",
            required = true, paramType = "path", dataType = "String")
    })
    @RequestMapping(value = "/refData/{tableId}/{columnCode}", method = RequestMethod.GET)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public Map<String, String> getColumnRefDictionary(@PathVariable String tableId, @PathVariable String columnCode,
                                               HttpServletRequest request) {
        return metaObjectService.fetchColumnRefData(tableId, columnCode,
            WebOptUtils.getCurrentTopUnit(request),
            WebOptUtils.getCurrentLang(request));
    }
}
