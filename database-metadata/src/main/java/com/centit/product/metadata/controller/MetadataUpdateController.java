package com.centit.product.metadata.controller;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.centit.fileserver.utils.UploadDownloadUtils;
import com.centit.framework.common.ResponseData;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaTable;
import com.centit.product.metadata.service.MetaDataService;
import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.StringRegularOpt;
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
import java.util.List;
import java.util.Map;

@Api(value = "数据库元数据信息完善", tags = "元数据信息维护")
@RestController
@RequestMapping(value = "update")
public class MetadataUpdateController extends BaseController {

    @Autowired
    private MetaDataService metaDataService;

    @ApiOperation(value = "获取所有业务数据类型Map")
    @GetMapping(value = {"/fieldType", "/no-auth/fieldType"})
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

    @ApiOperation(value = "导入TableStore中导入表关系")
    @ApiImplicitParam(name = "databaseCode", type = "path", value = "数据库ID")
    @RequestMapping(value = "/import/{databaseCode}", method = RequestMethod.POST)
    @WrapUpResponseBody
    public void importRelationsFromTableStore(@PathVariable String databaseCode, HttpServletRequest request) throws IOException {
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
    /* --- 批量操作接口 ：
        一 、设置某一个字段（字段属性名）的所有属性，包括：默认值生成规则、脱敏、校验、类型等等；
        ** 二、统一添加字段、 这个在重构接口中
        ** 三、统一删除字段、 这个在重构接口中
        四、统一设置表的属性，比如：逻辑删除等等
     */
    @ApiOperation(value = "批量修改表的属性，只能修改 逻辑删除标识、更新版本标识 、是否写入日志、是否全文检索。\r\n" +
        "这四个属性 :fulltextSearch writeOptLog deleteTagField  checkVersionField ")
    @PutMapping(value = "/batchUpdateTable")
    @ApiImplicitParam(name = "formJsonString", paramType="body", value = "JSON中分两部分，一部分是查询条件，一部分是修改的属性")
    @WrapUpResponseBody
    public void batchUpdateTableProps(@RequestBody String formJsonString){
        JSONObject formJson = JSONObject.parseObject(formJsonString);
        // filterType: database opt select
        // databaseCode
        // optId
        // tableIds []
        JSONObject filter = formJson.getJSONObject("filter");
        if(filter==null) return ;

        JSONObject props = formJson.getJSONObject("props");
        if(props==null || props.isEmpty()) return ;

        List<MetaTable> tables =  metaDataService.searchMateTable(filter);
        if(tables==null || tables.isEmpty()) return;
        // fulltextSearch writeOptLog deleteTagField  checkVersionField
        MetaTable tableInfo = new MetaTable();
        String fulltextSearch = props.getString("fulltextSearch");
        if(StringUtils.isNotBlank(fulltextSearch)) {
            tableInfo.setFulltextSearch(BooleanBaseOpt.castObjectToBoolean(fulltextSearch, false));
        }

        String writeOptLog = props.getString("writeOptLog");
        if(StringUtils.isNotBlank(writeOptLog)) {
            tableInfo.setWriteOptLog(BooleanBaseOpt.castObjectToBoolean(writeOptLog, false));
        }

        tableInfo.setDeleteTagField(StringRegularOpt.trimStringBlankAsNull(props.getString("deleteTagField")));
        tableInfo.setCheckVersionField(StringRegularOpt.trimStringBlankAsNull(props.getString("checkVersionField")));

        for(MetaTable metaTable : tables){
            tableInfo.setTableId(metaTable.getTableId());
            metaDataService.updateMetaTable(tableInfo);
        }
    }

    @ApiOperation(value = "批量修改表的字段属性；可以修改: 生成规则、脱敏、校验、应用、应用、延时加载 和 必填 等属性")
    @PutMapping(value = "/batchUpdateColumn")
    @WrapUpResponseBody
    public int batchUpdateTableColumns(@RequestBody String formJsonString){
        JSONObject formJson = JSONObject.parseObject(formJsonString);
        JSONObject filter = formJson.getJSONObject("filter");
        if(filter==null) return 0;

        JSONObject props = formJson.getJSONObject("props");
        if(props==null || props.isEmpty()) return 0;
        String columnName = props.getString("columnName");
        if(StringUtils.isBlank(columnName)){
            throw new ObjectException(ObjectException.DATA_VALIDATE_ERROR,
                "批量修改表的字段属性，必须指定字段名：columnName！");
        }

        List<MetaTable> tables =  metaDataService.searchMateTable(filter);
        if(tables==null || tables.isEmpty()) return 0;
        MetaColumn columnInfo = new MetaColumn();

        columnInfo.setColumnName(columnName);
        Object mandatory = props.get("mandatory");
        if(mandatory != null) {
            columnInfo.setMandatory(BooleanBaseOpt.castObjectToBoolean(mandatory, null));
        }
        Object lazyFetch = props.getString("lazyFetch");
        if(lazyFetch != null) {
            columnInfo.setLazyFetch(BooleanBaseOpt.castObjectToBoolean(lazyFetch, null));
        }
        columnInfo.setReferenceType(StringRegularOpt.trimStringBlankAsNull(props.getString("referenceType")));
        columnInfo.setReferenceData(StringRegularOpt.trimStringBlankAsNull(props.getString("referenceData")));
        columnInfo.setCheckRuleId(StringRegularOpt.trimStringBlankAsNull(props.getString("checkRuleId")));
        columnInfo.setCheckRuleParams(props.getJSONObject("checkRuleParams"));

        columnInfo.setAutoCreateCondition(StringRegularOpt.trimStringBlankAsNull(props.getString("autoCreateCondition")));
        columnInfo.setAutoCreateRule(StringRegularOpt.trimStringBlankAsNull(props.getString("autoCreateRule")));
        columnInfo.setAutoCreateParam(StringRegularOpt.trimStringBlankAsNull(props.getString("autoCreateParam")));
        columnInfo.setSensitiveType(StringRegularOpt.trimStringBlankAsNull(props.getString("sensitiveType")));
        int updated = 0;
        for(MetaTable metaTable : tables){
            MetaColumn column = metaDataService.getMetaColumn(metaTable.getTableId(), columnName);
            if(column!=null){
                columnInfo.setTableId(metaTable.getTableId());
                metaDataService.updateMetaColumn(columnInfo);
                updated++;
            }
        }
        return updated;
    }

}
