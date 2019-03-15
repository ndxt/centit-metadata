package com.centit.product.metadata.controller;

import com.centit.framework.common.ObjectException;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.core.controller.WrapUpContentType;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.framework.ip.po.DatabaseInfo;
import com.centit.framework.security.model.CentitUserDetails;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.utils.PageDesc;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaRelation;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.product.metadata.vo.MetaTableCascade;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@Api(value = "数据库元数据", tags = "元数据")
@RestController
@RequestMapping(value = "metadata")
public class MetadataController {

    @Autowired
    private MetaDataService metaDataService;

    @ApiOperation(value = "数据库列表")
    @GetMapping(value = "/databases")
    @WrapUpResponseBody
    public List<DatabaseInfo> databases(){
        return metaDataService.listDatabase();
    }

    @ApiOperation(value = "表元数据")
    @ApiImplicitParam(name = "databaseCode", value = "数据库代码")
    @GetMapping(value = "/{databaseCode}/tables")
    @WrapUpResponseBody
    public PageQueryResult<MetaTable> metaTables(@PathVariable String databaseCode, PageDesc pageDesc){
        List<MetaTable> list = metaDataService.listMetaTables(databaseCode, pageDesc);
        return PageQueryResult.createResultMapDict(list==null? Collections.emptyList():list,pageDesc);
    }

    @ApiOperation(value = "数据库表")
    @ApiImplicitParam(name = "databaseCode", value = "数据库ID")
    @GetMapping(value = "/{databaseCode}/db_tables")
    public List<SimpleTableInfo> databaseTables(@PathVariable String databaseCode){
        return metaDataService.listRealTables(databaseCode);
    }

    @ApiOperation(value = "同步数据库")
    @ApiImplicitParam(name = "databaseCode", value = "数据库ID")
    @GetMapping(value = "/{databaseCode}/synchronization")
    @WrapUpResponseBody
    public void syncDb(@PathVariable String databaseCode){
        CentitUserDetails userDetails = WebOptUtils.getLoginUser();
        if(userDetails == null){
            throw new ObjectException("未登录");
        }
        metaDataService.syncDb(databaseCode, userDetails.getUserCode());
    }

    @ApiOperation(value = "查询单个表元数据")
    @ApiImplicitParam(name = "tableId", value = "表ID")
    @GetMapping(value = "/table/{tableId}")
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public MetaTable getMetaTable(@PathVariable String tableId){
        return metaDataService.getMetaTable(tableId);
    }

    @ApiOperation(value = "查询列元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表ID")
    })
    @GetMapping(value = "/{tableId}/columns")
    @WrapUpResponseBody
    public PageQueryResult<MetaColumn> listColumns(@PathVariable String tableId, PageDesc pageDesc){
        List<MetaColumn> list = metaDataService.listMetaColumns(tableId, pageDesc);
        return PageQueryResult.createResult(list, pageDesc);
    }

    @ApiOperation(value = "查询单个列元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表元数据ID"),
        @ApiImplicitParam(name = "fieldLabelName", value = "列名")
    })
    @GetMapping(value = "/{tableId}/{columnName}")
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public MetaColumn getColumn(@PathVariable String tableId, @PathVariable String columnName){
        return metaDataService.getMetaColumn(tableId, columnName);
    }

    @ApiOperation(value = "修改表元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表ID"),
        @ApiImplicitParam(name = "tableLabelName", value = "中文名"),
        @ApiImplicitParam(name = "tableComment", value = "描述")
    })
    @PutMapping(value = "/table/{tableId}")
    @WrapUpResponseBody
    public void updateMetaTable(@PathVariable String tableId, String tableName, String tableComment, String tableState){
        CentitUserDetails userDetails = WebOptUtils.getLoginUser();
        if(userDetails == null){
            throw new ObjectException("未登录");
        }
        metaDataService.updateMetaTable(tableId, tableName, tableComment, tableState, userDetails.getUserCode());
    }

    @ApiOperation(value = "修改列元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表ID"),
        @ApiImplicitParam(name = "fieldLabelName", value = "列名")
    })
    @PutMapping(value = "/column/{tableId}/{columnCode}")
    @WrapUpResponseBody
    public void updateMetaColumns(@PathVariable String tableId, @PathVariable String columnCode, MetaColumn metaColumn){
        metaColumn.setTableId(tableId);
        metaColumn.setColumnName(columnCode);
        metaDataService.updateMetaColumn(metaColumn);
    }

    @ApiOperation(value = "查询关联关系元数据")
    @GetMapping(value = "/{tableId}/relations")
    @WrapUpResponseBody
    public PageQueryResult<MetaRelation> metaRelation(@PathVariable String tableId, PageDesc pageDesc){
        List<MetaRelation> list = metaDataService.listMetaRelation(tableId, pageDesc);
        return PageQueryResult.createResultMapDict(list, pageDesc);
    }

    @ApiOperation(value = "新建关联关系元数据")
    @PostMapping(value = "/{tableId}/relation")
    @WrapUpResponseBody
    public void createRelations(@PathVariable String tableId, MetaTable metaTable){
        metaDataService.saveRelations(tableId, metaTable.getMdRelations());
    }

    @ApiOperation(value = "元数据级联字段，只查询一层")
    @GetMapping(value = "/tablecascade/{tableId}/{tableAlias}")
    @WrapUpResponseBody
    public MetaTableCascade getMetaTableCascade(@PathVariable String tableId, @PathVariable String tableAlias){
        return metaDataService.getMetaTableCascade(tableId, tableAlias);
    }
}
