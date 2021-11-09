package com.centit.product.metadata.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.fileserver.utils.UploadDownloadUtils;
import com.centit.framework.common.ResponseData;
import com.centit.framework.components.OperationLogCenter;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaDataCache;
import com.centit.product.metadata.service.MetaObjectService;
import com.centit.product.metadata.transaction.MetadataJdbcTransaction;
import com.centit.search.service.Impl.ESSearcher;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.PageDesc;
import com.centit.support.file.FileIOOpt;
import com.centit.support.file.FileType;
import com.centit.support.report.ExcelExportUtil;
import com.centit.support.report.ExcelImportUtil;
import com.centit.support.report.ExcelTypeEnum;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;

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
    @Autowired(required = false)
    private ESSearcher esObjectSearcher;

    @ApiOperation(value = "分页查询数据库表数据列表")
    @RequestMapping(value = "/{tableId}/list", method = RequestMethod.GET)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public PageQueryResult<Object> listObjects(@PathVariable String tableId, PageDesc pageDesc,
                                               String[] fields, HttpServletRequest request) {
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
        Map<String, Object> parameters = collectRequestParameters(request);
        return metaObjectService.getObjectById(tableId, parameters);
    }

    @ApiOperation(value = "修改数据库表数据")
    @RequestMapping(value = "/{tableId}", method = RequestMethod.PUT)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData updateObject(@PathVariable String tableId,
                                     @RequestBody String jsonString) {
        metaObjectService.updateObject(tableId, JSON.parseObject(jsonString));
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "根据属性修改数据库表数据")
    @RequestMapping(value = "/{tableId}/batch", method = RequestMethod.PUT)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData batchUpdateObject(@PathVariable String tableId,
                                          @RequestBody String fieldsObject, HttpServletRequest request) {
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
    public ResponseData batchDeleteObject(@PathVariable String tableId, @RequestBody String primaryArray, Integer withChildrenDeep) {
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
                                   @RequestBody String jsonString) {
        metaObjectService.saveObject(tableId, JSON.parseObject(jsonString));
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "删除数据库表数据")
    @RequestMapping(value = "/{tableId}", method = RequestMethod.DELETE)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData deleteObject(@PathVariable String tableId,
                                     HttpServletRequest request) {
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
                                                                  Integer withChildrenDeep) {
        JSONArray jsonArray = JSON.parseArray(mergeArray);
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        List<Map<String, Object>> list = new ArrayList<>();
        jsonArray.forEach(object -> {
            innerMergeObject(tableId, tableInfo, (JSONObject) object, withChildrenDeep == null ? 1 : withChildrenDeep);
            Map<String, Object> primaryKey = tableInfo.fetchObjectPk((JSONObject) object);
            list.add(primaryKey);
        });
        return list;
    }

    private void innerMergeObject(String tableId, MetaTable tableInfo, JSONObject object, Integer withChildrenDeep) {
        Map<String, Object> dbObjectPk = tableInfo.fetchObjectPk(object);
        Map<String, Object> dbObject = dbObjectPk == null ? null :
            metaObjectService.getObjectById(tableId, dbObjectPk);
        if (dbObject == null) {
            metaObjectService.saveObjectWithChildren(tableId, object, withChildrenDeep);
        } else {
            metaObjectService.updateObjectWithChildren(tableId, object, withChildrenDeep);
        }
    }

    @ApiOperation(value = "获取一个数据带子表，主键作为参数以key-value形式提交")
    @RequestMapping(value = "/{tableId}/withChildren", method = RequestMethod.GET)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public Map<String, Object> getObjectWithChildren(@PathVariable String tableId, Integer withChildrenDeep,
                                                     HttpServletRequest request) {
        Map<String, Object> parameters = collectRequestParameters(request);
        return metaObjectService.getObjectWithChildren(tableId, parameters, withChildrenDeep == null ? 1 : withChildrenDeep);
    }

    @ApiOperation(value = "修改数据库表数据带子表")
    @RequestMapping(value = "/{tableId}/withChildren", method = RequestMethod.PUT)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData updateObjectWithChildren(@PathVariable String tableId, Integer withChildrenDeep,
                                                 @RequestBody String jsonString, HttpServletRequest request) {
        metaObjectService.updateObjectWithChildren(tableId, JSON.parseObject(jsonString), withChildrenDeep == null ? 1 : withChildrenDeep);
        saveOperationLog(request, jsonString, tableId, "update");
        return ResponseData.makeSuccessResponse();
    }

    @ApiOperation(value = "新增数据库表数据带子表")
    @RequestMapping(value = "/{tableId}/withChildren", method = RequestMethod.POST)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public ResponseData saveObjectWithChildren(@PathVariable String tableId, Integer withChildrenDeep,
                                               @RequestBody String jsonString, HttpServletRequest request) {
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
        Map<String, Object> parameters = collectRequestParameters(request);
        metaObjectService.deleteObjectWithChildren(tableId, parameters, withChildrenDeep == null ? 1 : withChildrenDeep);
        saveOperationLog(request, parameters, tableId, "delete");
        return ResponseData.makeSuccessResponse();
    }

    private void saveOperationLog(HttpServletRequest request, Object newValue, String optTag, String optMethod) {
        MetaTable tableInfo = metaDataCache.getTableInfo(optTag);
        if (tableInfo != null && tableInfo.isWriteOptLog()) {
            OperationLogCenter.log(request, "0",
                optTag, "metaData", optMethod, "元数据", newValue, null);
        }
    }

    @ApiOperation(value = "全文检索")
    @ApiImplicitParams({@ApiImplicitParam(
        name = "tableId", value = "表单模块id",
        required = true, paramType = "path", dataType = "String"
    ), @ApiImplicitParam(
        name = "query", value = "检索关键字",
        required = true, paramType = "query", dataType = "String"
    )})
    @RequestMapping(value = "/{tableId}/search", method = RequestMethod.GET)
    @WrapUpResponseBody
    @MetadataJdbcTransaction
    public PageQueryResult<Map<String, Object>> searchObject(@PathVariable String tableId,
                                                             HttpServletRequest request, PageDesc pageDesc) {
        if (esObjectSearcher == null) {
            throw new ObjectException(ObjectException.SYSTEM_CONFIG_ERROR, "没有正确配置Elastic Search");
        }
        Map<String, Object> queryParam = collectRequestParameters(request);
        Map<String, Object> searchQuery = new HashMap<>(10);
        String queryWord = StringBaseOpt.castObjectToString(queryParam.get("query"));
        searchQuery.put("optId", tableId);
        Object user = queryParam.get("userCode");
        if (user != null) {
            searchQuery.put("userCode", StringBaseOpt.castObjectToString(user));
        }
        Object units = queryParam.get("unitCode");
        if (units != null) {
            searchQuery.put("unitCode", StringBaseOpt.objectToStringArray(units));
        }

        Pair<Long, List<Map<String, Object>>> res =
            esObjectSearcher.search(searchQuery, queryWord, pageDesc.getPageNo(), pageDesc.getPageSize());
        if (res == null) {
            throw new ObjectException("ELK异常");
        }
        pageDesc.setTotalRows(NumberBaseOpt.castObjectToInteger(res.getLeft()));
        return PageQueryResult.createResult(res.getRight(), pageDesc);
    }

    @ApiOperation(value = "导出数据库表数据列表可分页，传入表id")
    @RequestMapping(value = "/{tableId}/export", method = RequestMethod.GET)
    @MetadataJdbcTransaction
    public void exportObjects(@PathVariable String tableId, PageDesc pageDesc,
                              HttpServletRequest request, HttpServletResponse response) throws IOException {

        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        if (null == tableId) {
            throw new ObjectException("没有对应的表元数据");
        }
        JSONArray jsonArray = metaObjectService.pageQueryObjects(tableId, collectRequestParameters(request), pageDesc);
        List<String> columnNames = tableInfo.getColumns().stream().map(MetaColumn::getPropertyName).collect(Collectors.toList());
        InputStream excelStream = ExcelExportUtil.generateExcelStream(jsonArray,
            CollectionsOpt.listToArray(columnNames), CollectionsOpt.listToArray(columnNames));
        String fileName = URLEncoder.encode(tableInfo.getTableName(), "UTF-8") +
            pageDesc.getRowStart() + "-" + pageDesc.getRowEnd() + "-" + pageDesc.getTotalRows() +
            ".xlsx";
        response.setContentType(FileType.mapExtNameToMimeType("xlsx"));
        response.setHeader("Content-disposition", "attachment; filename=" + fileName);
        IOUtils.copy(excelStream, response.getOutputStream());

    }

    @ApiOperation(value = "导入数据库表数据，传入表id")
    @RequestMapping(value = "/{tableId}/import", method = RequestMethod.POST)
    @MetadataJdbcTransaction
    public ResponseData importObjects(@PathVariable String tableId, HttpServletRequest request) throws IOException {

        Pair<String, InputStream> fileInfo = UploadDownloadUtils.fetchInputStreamFromMultipartResolver(request);
        List<Map<String, Object>> maps = ExcelImportUtil.loadMapFromExcelSheet(fileInfo.getValue(), 0);
        //剔除数据全为空的数据
        maps = maps.stream().filter(map -> {
            boolean b = false;
            for (Object value : map.values()) {
                if (null != value && (value instanceof String && StringUtils.isNotBlank((String) value))) {
                    b = true;
                }
            }
            return b;
        }).collect(Collectors.toList());
        //去除有自动生成策略字段中值为空字符串的数据
        MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
        List<String> autoCreatedColumns = tableInfo.getColumns().stream().filter(column -> StringUtils.isNotBlank(column.getAutoCreateRule())).map(MetaColumn::getPropertyName).collect(Collectors.toList());
        maps.forEach(map -> autoCreatedColumns.forEach(column -> {
                if (StringUtils.isEmpty(MapUtils.getString(map, column))) {
                    map.remove(column);
                }
            }
        ));
        maps.forEach(map -> metaObjectService.mergeObjectWithChildren(tableId, map, 1));
        return ResponseData.makeSuccessResponse();
    }

}
