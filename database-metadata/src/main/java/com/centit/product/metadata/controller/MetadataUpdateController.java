package com.centit.product.metadata.controller;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.centit.fileserver.utils.UploadDownloadUtils;
import com.centit.framework.common.ResponseData;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.product.adapter.po.MetaColumn;
import com.centit.product.adapter.po.MetaTable;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.support.common.ObjectException;
import com.centit.support.database.utils.FieldType;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
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


    @ApiOperation(value = "修改表元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表ID"),
        @ApiImplicitParam(name = "tableLabelName", value = "中文名"),
        @ApiImplicitParam(name = "tableComment", value = "描述")
    })
    @PutMapping(value = "/table/{tableId}")
    @WrapUpResponseBody
    public void updateMetaTable(@PathVariable String tableId, @RequestBody MetaTable metaTable, HttpServletRequest request){
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
        if("A".equals(metaColumn.getAutoCreateRule()) && !metaColumn.isPrimaryKey()){
            throw new ObjectException(ObjectException.DATA_VALIDATE_ERROR, "自动增长生成规则只能配置在主键上！");
        }
        metaDataService.updateMetaColumn(metaColumn);
    }

    @ApiOperation(value = "新建关联关系元数据")
    @PostMapping(value = "/{tableId}/relation")
    @WrapUpResponseBody
    public void createRelations(@PathVariable String tableId, MetaTable metaTable){
        metaDataService.saveRelations(tableId, metaTable.getMdRelations());
    }

    @ApiOperation(value = "导入TableStore中导入表结构")
    @ApiImplicitParam(name = "databaseCode", type = "path", value = "数据库ID")
    @RequestMapping(value = "/import/{databaseCode}", method = RequestMethod.POST)
    @WrapUpResponseBody
    public void importFromTableStore(@PathVariable String databaseCode, HttpServletRequest request) throws IOException {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        if(StringUtils.isBlank(userCode)){
            throw new ObjectException(ResponseData.ERROR_FORBIDDEN, "用户没登录，或者session已失效！");
        }
        Pair<String, InputStream> fileInfo = UploadDownloadUtils.fetchInputStreamFromMultipartResolver(request);
        JSONObject jsonObject = JSON.parseObject(fileInfo.getRight());
        if(jsonObject!=null && //检验json的合法性
            jsonObject.containsKey("projectInfo") && jsonObject.containsKey("tables") && jsonObject.containsKey("modules")){
            metaDataService.importRelationFromTableStore(databaseCode, jsonObject, userCode);
        } else {
            throw new ObjectException(ObjectException.DATA_VALIDATE_ERROR, "文件中的json格式不正确！");
        }
    }
}
