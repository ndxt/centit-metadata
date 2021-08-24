package com.centit.product.metadata.controller;

import com.centit.framework.common.ResponseData;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.po.SourceInfo;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.support.database.utils.FieldType;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@Api(value = "数据库元数据信息完善", tags = "元数据信息完善")
@RestController
@RequestMapping(value = "update")
public class MetadataUpdateController extends BaseController {

    @Autowired
    private MetaDataService metaDataService;

    @ApiOperation(value = "获取所有业务数据类型Map")
    @GetMapping(value = "/fieldType")
    @WrapUpResponseBody
    public Map<String, String> listFieldType(){
        return FieldType.getAllTypeMap();
    }

    @ApiOperation(value = "同步数据库")
    @ApiImplicitParam(name = "databaseCode", value = "数据库ID")
    @GetMapping(value = "/sync/{databaseCode}")
    @WrapUpResponseBody
    public ResponseData syncDb(@PathVariable String databaseCode,String tableName, HttpServletRequest request){
        SourceInfo databaseInfo = metaDataService.getDatabaseInfo(databaseCode);
        //先写死，后面在表中加个字段或者放配置文件中，不然后面每加一个就需要更改
        if (databaseInfo!=null&&("H".equals(databaseInfo.getSourceType()) || "R".equals(databaseInfo.getSourceType()))){
            return ResponseData.makeErrorMessage("选择的资源不支持反向工程！");
        }
        String userCode = WebOptUtils.getCurrentUserCode(request);
        metaDataService.syncDb(databaseCode, userCode,tableName);
        return ResponseData.makeSuccessResponse();
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
