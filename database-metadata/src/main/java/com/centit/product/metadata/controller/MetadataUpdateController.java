package com.centit.product.metadata.controller;

import com.centit.framework.common.WebOptUtils;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaDataService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@Api(value = "数据库元数据信息完善", tags = "元数据信息完善")
@RestController
@RequestMapping(value = "update")
public class MetadataUpdateController extends BaseController {

    @Autowired
    private MetaDataService metaDataService;


    @ApiOperation(value = "同步数据库")
    @ApiImplicitParam(name = "databaseCode", value = "数据库ID")
    @GetMapping(value = "/sync/{databaseCode}")
    @WrapUpResponseBody
    public void syncDb(@PathVariable String databaseCode, HttpServletRequest request){
        String userCode = WebOptUtils.getCurrentUserCode(request);

        metaDataService.syncDb(databaseCode, userCode);
    }


    @ApiOperation(value = "修改表元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表ID"),
        @ApiImplicitParam(name = "tableLabelName", value = "中文名"),
        @ApiImplicitParam(name = "tableComment", value = "描述")
    })
    @PutMapping(value = "/table/{tableId}")
    @WrapUpResponseBody
    public void updateMetaTable(@PathVariable String tableId, @RequestBody MetaTable metaTable,HttpServletRequest request){
        String userCode = WebOptUtils.getCurrentUserCode(request);

        metaTable.setTableId(tableId);
        metaTable.setRecorder(userCode);
        metaDataService.updateMetaTable(metaTable);
    }

    @ApiOperation(value = "修改列元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表ID"),
        @ApiImplicitParam(name = "fieldLabelName", value = "列名")
    })
    @PutMapping(value = "/column/{tableId}/{columnCode}")
    @WrapUpResponseBody
    public void updateMetaColumns(@PathVariable String tableId, @PathVariable String columnCode, @RequestBody MetaColumn metaColumn){
        metaColumn.setTableId(tableId);
        metaColumn.setColumnName(columnCode);
        metaDataService.updateMetaColumn(metaColumn);
    }

    @ApiOperation(value = "新建关联关系元数据")
    @PostMapping(value = "/{tableId}/relation")
    @WrapUpResponseBody
    public void createRelations(@PathVariable String tableId, MetaTable metaTable){
        metaDataService.saveRelations(tableId, metaTable.getMdRelations());
    }

}
