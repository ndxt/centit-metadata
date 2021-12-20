package com.centit.product.metadata.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.components.CodeRepositoryUtil;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpContentType;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.product.adapter.po.MetaColumn;
import com.centit.product.adapter.po.MetaRelation;
import com.centit.product.adapter.po.MetaTable;
import com.centit.product.adapter.po.SourceInfo;
import com.centit.product.metadata.service.MetaDataCache;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.product.metadata.transaction.AbstractSourceConnectThreadHolder;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Api(value = "数据库元数据查询", tags = "元数据查询")
@RestController
@RequestMapping(value = "query")
public class MetadataQueryController extends BaseController {

    @Autowired
    private MetaDataService metaDataService;

    @Autowired
    private MetaDataCache metaDataCache;

    @ApiOperation(value = "数据库列表")
    @GetMapping(value = "/databases")
    @WrapUpResponseBody
    public List<SourceInfo> databases(String osId, HttpServletRequest request) {
        Map<String, Object> parameters = collectRequestParameters(request);
        String topUnit = WebOptUtils.getCurrentTopUnit(request);
        //如果用户还未加入到任何租户，不进行数据查询操作
        if (StringUtils.isBlank(topUnit)){
            return Collections.emptyList();
        }
        if (WebOptUtils.isTenantTopUnit(request)) {
            parameters.put("topUnit", WebOptUtils.getCurrentTopUnit(request));
        }
        return metaDataService.listDatabase(parameters);
    }

    @ApiOperation(value = "数据库中表分页查询")
    @ApiImplicitParam(name = "databaseCode", value = "数据库代码")
    @GetMapping(value = "/{databaseCode}/tables")
    @WrapUpResponseBody
    public PageQueryResult<Object> metaTables(@PathVariable String databaseCode, PageDesc pageDesc, HttpServletRequest request) {
        Map<String, Object> searchColumn = collectRequestParameters(request);
        searchColumn.put("databaseCode", databaseCode);
        JSONArray list = metaDataService.listMetaTables(searchColumn, pageDesc);
        return PageQueryResult.createJSONArrayResult(list, pageDesc, MetaTable.class);
    }

    @ApiOperation(value = "查询单个表元数据")
    @ApiImplicitParam(name = "tableId", value = "表ID")
    @GetMapping(value = "/table/{tableId}")
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public MetaTable getMetaTable(@PathVariable String tableId) {
        return metaDataService.getMetaTable(tableId);
    }

    @ApiOperation(value = "查询单个表元数据(包括字段信息和关联表信息)")
    @ApiImplicitParam(name = "tableId", value = "表ID")
    @GetMapping(value = "/table/{tableId}/all")
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public MetaTable getMetaTableWithRelations(@PathVariable String tableId) {
        return metaDataService.getMetaTableWithRelations(tableId);
    }

    @ApiOperation(value = "查询列元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表ID")
    })
    @GetMapping(value = "/{tableId}/columns")
    @WrapUpResponseBody
    public PageQueryResult<MetaColumn> listColumns(@PathVariable String tableId, PageDesc pageDesc) {
        List<MetaColumn> list = metaDataService.listMetaColumns(tableId, pageDesc);
        return PageQueryResult.createResult(list, pageDesc);
    }

    @ApiOperation(value = "查询单个列元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表元数据ID"),
        @ApiImplicitParam(name = "columnName", value = "列名")
    })
    @GetMapping(value = "/{tableId}/column/{columnName}")
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public MetaColumn getColumn(@PathVariable String tableId, @PathVariable String columnName) {
        return metaDataService.getMetaColumn(tableId, columnName);
    }

    @ApiOperation(value = "查询关联关系元数据")
    @GetMapping(value = "/{tableId}/relations")
    @WrapUpResponseBody
    public PageQueryResult<MetaRelation> metaRelation(@PathVariable String tableId, HttpServletRequest request, PageDesc pageDesc) {
        Map<String, Object> condition = BaseController.collectRequestParameters(request);
        condition.put("parentTableId", tableId);
        List<MetaRelation> list = metaDataService.listMetaRelation(condition, pageDesc);
        return PageQueryResult.createResultMapDict(list, pageDesc);
    }


    @ApiOperation(value = "查询单个列参照数据， REFERENCE_TYPE！=‘0’有效")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表元数据ID"),
        @ApiImplicitParam(name = "columnName", value = "列名")
    })
    @GetMapping(value = "/{tableId}/reference/{columnName}")
    @WrapUpResponseBody
    public Map<String, String> getColumnReferenceData(
        @PathVariable String tableId, @PathVariable String columnName,
        HttpServletRequest request) {
        MetaColumn col = metaDataService.getMetaColumn(tableId, columnName);
        if (col == null || StringUtils.isBlank(col.getReferenceType()) || "0".equals(col.getReferenceType())) {
            return null;
        }
        //1： 数据字典 2：JSON表达式 3：sql语句  Y：年份 M：月份
        switch (col.getReferenceType()) {
            case "1":
                return CodeRepositoryUtil.getLabelValueMap(col.getReferenceData());
            case "2":
                return CollectionsOpt.objectMapToStringMap(CollectionsOpt.objectToMap(
                    JSON.parse(col.getReferenceData())));
            case "3":
                Map<String, Object> searchColumn = collectRequestParameters(request);
                MetaTable tableInfo = metaDataCache.getTableInfo(tableId);
                SourceInfo sourceInfo = metaDataService.getDatabaseInfo(tableInfo.getDatabaseCode());
                try {
                    Connection conn = AbstractSourceConnectThreadHolder.fetchConnect(sourceInfo);
                    QueryAndNamedParams qAp = QueryUtils.translateQuery(col.getReferenceData(), searchColumn);
                    List<Object[]> objects = DatabaseAccess.findObjectsBySql(conn, qAp.getQuery(), qAp.getParams());
                    if (objects != null) {
                        Map<String, String> stringMap = new HashMap<>(objects.size() * 3 / 2 + 1);
                        for (Object[] objs : objects) {
                            if (objs != null && objs.length >= 2) {
                                stringMap.put(StringBaseOpt.objectToString(objs[0]),
                                    StringBaseOpt.objectToString(objs[1]));
                            }
                        }
                        return stringMap;
                    }
                } catch (SQLException | IOException e) {
                    throw new ObjectException(col, PersistenceException.DATABASE_OPERATE_EXCEPTION, e);
                }
                return null;
            case "Y": {
                Map<String, String> stringMap = new HashMap<>(120);
                for (int i = 1; i < 100; i++) {
                    stringMap.put(String.valueOf(1950 + i), String.valueOf(1950 + i));
                }
                return stringMap;
            }
            case "M": {
                Map<String, String> stringMap = new HashMap<>(100);
                for (int i = 1; i < 13; i++) {
                    stringMap.put(String.valueOf(i), String.valueOf(i));
                }
                return stringMap;
            }
            default:
                return null;
        }
    }
}
