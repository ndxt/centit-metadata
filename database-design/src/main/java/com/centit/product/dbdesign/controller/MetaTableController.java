package com.centit.product.dbdesign.controller;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.centit.fileserver.utils.SystemTempFileUtils;
import com.centit.fileserver.utils.UploadDownloadUtils;
import com.centit.framework.common.JsonResultUtils;
import com.centit.framework.common.ResponseData;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpContentType;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.DictionaryMapColumn;
import com.centit.framework.core.dao.DictionaryMapUtils;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.product.dbdesign.service.MetaChangLogManager;
import com.centit.product.dbdesign.service.MetaTableManager;
import com.centit.product.dbdesign.service.TranslateColumn;
import com.centit.product.metadata.po.MetaChangLog;
import com.centit.product.metadata.po.PendingMetaColumn;
import com.centit.product.metadata.po.PendingMetaTable;
import com.centit.product.metadata.utils.PdmTableInfoUtils;
import com.centit.support.common.ObjectException;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.utils.PageDesc;
import com.centit.support.file.FileIOOpt;
import com.centit.support.file.FileSystemOpt;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * MdTable  Controller.
 * create by scaffold 2016-06-02
 * 表元数据表状态分为 系统/查询/更新
 * 系统，不可以做任何操作
 * 查询，仅用于通用查询模块，不可以更新
 * 更新，可以更新
 */
@Controller
@RequestMapping("/mdtable")
@Api(value = "数据重构", tags = "数据重构")
public class MetaTableController extends BaseController {
    //private static final Log log = LogFactory.getLog(MetaTableController.class);

    @Resource
    private MetaTableManager metaTableManager;

    @Resource
    private MetaChangLogManager metaChangLogManager;

    @Resource
    private TranslateColumn translateColumn;

    @ApiOperation(value = "查询重构记录")
    @RequestMapping(value = "/{databaseCode}/log", method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult loglist(@PathVariable String databaseCode, String[] field, PageDesc pageDesc,
                                   HttpServletRequest request) {
        Map<String, Object> searchColumn = collectRequestParameters(request);
        searchColumn.put("databaseCode", databaseCode);
        JSONArray listObjects = metaChangLogManager.listMdChangLogsAsJson(field, searchColumn, pageDesc);
        if (ArrayUtils.isNotEmpty(field)) {
            return PageQueryResult.createJSONArrayResult(listObjects, pageDesc, field, MetaChangLog.class);
        } else {
            return PageQueryResult.createJSONArrayResult(listObjects, pageDesc, MetaChangLog.class);
        }
    }

    @ApiOperation(value = "查看单个表重构记录")
    @RequestMapping(value = "/log/{changeId}", method = {RequestMethod.GET})
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public MetaChangLog getMdTableLog(@PathVariable String changeId) {
        return metaTableManager.getMetaChangLog(changeId);
    }

    @ApiOperation(value = "查询单个表重构字段")
    @RequestMapping(value = "/{tableId}", method = {RequestMethod.GET})
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public PendingMetaTable getMdTableDraft(@PathVariable String tableId, HttpServletRequest request) {
        PendingMetaTable mdTable = metaTableManager.getPendingMetaTable(tableId);
        if (null != mdTable) {
            if (null == mdTable.getColumns()) {
                mdTable.setMdColumns(new ArrayList<>());
            }
            return mdTable;
        }
        //如果mdTable数据为空，重新用MetaTable数据初始化
        mdTable = metaTableManager.initPendingMetaTable(tableId, WebOptUtils.getCurrentUserCode(request));
        return mdTable;
    }

    @ApiOperation(value = "新增重构表")
    @RequestMapping(method = {RequestMethod.POST})
    public void createMdTable(PendingMetaTable mdTable, HttpServletRequest request,
                              HttpServletResponse response) {

        boolean isExist = metaTableManager.isTableExist(mdTable.getTableName(), mdTable.getDatabaseCode());
        String userCode = WebOptUtils.getCurrentUserCode(request);
        mdTable.setRecorder(userCode);
        mdTable.setTableState("W");
        PendingMetaTable table = new PendingMetaTable();
        table.copyNotNullProperty(mdTable);
        if (!isExist) {
            metaTableManager.saveNewPendingMetaTable(table);
            JsonResultUtils.writeSingleDataJson(table.getTableId(), response);
        } else {
            if ("V".equals(mdTable.getTableType())) {
                metaTableManager.savePendingMetaTable(table);
                JsonResultUtils.writeSingleDataJson(table.getTableId(), response);
            } else {
                JsonResultUtils.writeErrorMessageJson(800, mdTable.getTableName() + "已存在", response);
            }
        }
    }

    @ApiOperation(value = "修改重构表")
    @PutMapping(value = "/table/{tableId}")
    @WrapUpResponseBody
    public void updateMetaTable(@PathVariable String tableId, @RequestBody PendingMetaTable metaTable, HttpServletRequest request) {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        metaTable.setTableId(tableId);
        metaTable.setRecorder(userCode);
        metaTableManager.updateMetaTable(metaTable);
    }

    @ApiOperation(value = "修改重构表字段")
    @PutMapping(value = "/column/{tableId}/{columnCode}")
    @WrapUpResponseBody
    public void updateMetaColumns(@PathVariable String tableId, @PathVariable String columnCode,
                                  @RequestBody PendingMetaColumn metaColumn) {
        metaColumn.setTableId(tableId);
        metaColumn.setColumnName(columnCode);
        metaTableManager.updateMetaColumn(metaColumn);
    }

    @ApiOperation(value = "编辑重构表")
    @RequestMapping(value = "/{tableId}", method = {RequestMethod.PUT})
    @WrapUpResponseBody
    public void updateMdTable(@PathVariable String tableId, @RequestBody PendingMetaTable mdTable) {
        mdTable.setTableId(tableId);
        List<String> alterSqls = metaTableManager.makeAlterTableSqlList(mdTable);
        mdTable.setTableState(alterSqls.size() > 0 ? "W" : "S");
        metaTableManager.savePendingMetaTable(mdTable);
    }

    @ApiOperation(value = "删除重构表")
    @RequestMapping(value = "/{tableId}", method = {RequestMethod.DELETE})
    @WrapUpResponseBody
    public void deleteMdTable(@PathVariable String tableId) {
        metaTableManager.deletePendingMetaTable(tableId);
    }

    @ApiOperation(value = "查看发布重构表sql")
    @RequestMapping(value = "/beforePublish/{pendingTableId}", method = {RequestMethod.POST})
    @WrapUpResponseBody
    public ResponseData alertSqlBeforePublish(@PathVariable String pendingTableId) {
        List<String> sqlList = metaTableManager.makeAlterTableSqlList(pendingTableId);
        if (null == sqlList) {
            return ResponseData.makeErrorMessage(601, "表字段不能为空");
        }
        return ResponseData.makeResponseData(sqlList);
    }

    @ApiOperation(value = "发布重构表")
    @RequestMapping(value = "/publish/{pendingTableId}", method = {RequestMethod.POST})
    public void publishMdTable(@PathVariable String pendingTableId,
                               HttpServletRequest request, HttpServletResponse response) {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        Pair<Integer, String> ret = metaTableManager.publishMetaTable(pendingTableId, userCode);
        JSONObject json = translatePublishMessage(ret);
        JsonResultUtils.writeSingleDataJson(json, response);
    }

    @ApiOperation(value = "range")
    @CrossOrigin(origins = "*", allowCredentials = "true", maxAge = 86400, methods = RequestMethod.GET)
    @RequestMapping(value = "/range", method = {RequestMethod.GET})
    @WrapUpResponseBody
    public JSONObject checkFileRange(String token, long size) {
        //FileRangeInfo fr = new FileRangeInfo(token,size);
        //Pair<String, InputStream> fileInfo = UploadDownloadUtils.fetchInputStreamFromMultipartResolver(request);
        //检查临时目录中的文件大小，返回文件的其实点
        String tempFilePath = SystemTempFileUtils.getTempFilePath(token, size);
        long tempFileSize = SystemTempFileUtils.checkTempFileSize(tempFilePath);
        Map<String, Object> data = new HashMap<>(4);
        data.put("tempFilePath", token + "_" + size);
        JSONObject jsonObject = UploadDownloadUtils.makeRangeUploadJson(tempFileSize, token, token + "_" + size);
        if (tempFileSize == size) {
            data.put("tables", fetchPdmTables(tempFilePath));
            jsonObject.put("tables", data);
        }
        return jsonObject;
    }

    @ApiOperation(value = "导入pdm返回表数据")
    @CrossOrigin(origins = "*", allowCredentials = "true", maxAge = 86400, methods = RequestMethod.POST)
    @RequestMapping(value = "/range", method = {RequestMethod.POST})
    public void syncPdm(String token, long size,
                        HttpServletRequest request, HttpServletResponse response)
        throws IOException {

        Pair<String, InputStream> fileInfo = UploadDownloadUtils.fetchInputStreamFromMultipartResolver(request);
        FileSystemOpt.createDirect(SystemTempFileUtils.getTempDirectory());
        String tempFilePath = SystemTempFileUtils.getTempFilePath(token, size);
        try (FileOutputStream out = new FileOutputStream(new File(tempFilePath))) {
            long uploadSize = FileIOOpt.writeInputStreamToOutputStream(fileInfo.getRight(), out);
            if (uploadSize > 0) {
                //上传到临时区成功
                JSONObject jsonObject = new JSONObject();
                Map<String, Object> data = new HashMap<>(4);
                data.put("tempFilePath", token + "_" + size);
                data.put("tables", PdmTableInfoUtils.getTableNameFromPdm(tempFilePath));
                jsonObject.put("tables", data);
                JsonResultUtils.writeSingleDataJson(jsonObject, response);
            } else {
                JsonResultUtils.writeOriginalJson(UploadDownloadUtils.
                    makeRangeUploadJson(uploadSize, token, token + "_" + size).toJSONString(), response);
            }

        } catch (ObjectException e) {
            logger.error(e.getMessage(), e);
            JsonResultUtils.writeHttpErrorMessage(e.getExceptionCode(),
                e.getMessage(), response);
        }
    }

    @ApiOperation(value = "确认导入pdm修改表元数据表")
    @RequestMapping(value = "/{databaseCode}/confirm", method = {RequestMethod.POST})
    @WrapUpResponseBody
    public void syncConfirm(@PathVariable String databaseCode, @RequestBody String data,
                            HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> params = collectRequestParameters(request);
        JSONObject object = JSON.parseObject(data);
        object.putAll(params);
        String tempFilePath = SystemTempFileUtils.getTempDirectory() + object.getString("tempFilePath") + ".tmp";
        JSONArray jsonArray = object.getJSONArray("data");
        List<String> tables = new ArrayList<>();
        for (Object o : jsonArray) {
            tables.add(o.toString());
        }
        String userCode = WebOptUtils.getCurrentUserCode(request);
        Pair<Integer, String> ret = metaTableManager.syncPdm(databaseCode, tempFilePath, tables, userCode);
        JsonResultUtils.writeErrorMessageJson(ret.getLeft(), ret.getRight(), response);
    }

    @ApiOperation(value = "批量发布表元数据表")
    @RequestMapping(value = "/{databaseCode}/publish", method = {RequestMethod.POST})
    @WrapUpResponseBody
    public void publishDatabase(@PathVariable String databaseCode,
                                HttpServletRequest request, HttpServletResponse response) {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        Pair<Integer, String> ret = metaTableManager.publishDatabase(databaseCode, userCode);
        JSONObject json = translatePublishMessage(ret);
        JsonResultUtils.writeSingleDataJson(json, response);
    }

    @ApiOperation(value = "查询表列数据元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表ID")
    })
    @GetMapping(value = "/{tableId}/columns")
    @WrapUpResponseBody
    public PageQueryResult<PendingMetaColumn> listColumns(@PathVariable String tableId, PageDesc pageDesc) {
        List<PendingMetaColumn> list = metaTableManager.listMetaColumns(tableId, pageDesc);
        return PageQueryResult.createResult(list, pageDesc);
    }

    @ApiOperation(value = "查询单个列表元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表元数据ID"),
        @ApiImplicitParam(name = "columnName", value = "列名")
    })
    @GetMapping(value = "/{tableId}/column/{columnName}")
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public PendingMetaColumn getColumn(@PathVariable String tableId, @PathVariable String columnName) {
        return metaTableManager.getMetaColumn(tableId, columnName);
    }

    @ApiOperation(value = "查询列元数据,pending表与md表数据的组合,通过osId,dataBaseCode过滤,如果osId和dataBaseCode不传,后端根据topUnit过滤)")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "databaseCode", value = "数据库code", paramType = "query"),
        @ApiImplicitParam(name = "optId", value = "操作id", paramType = "query"),
        @ApiImplicitParam(name = "likeTableNameOrLabel", value = "根据表代码或表名模糊过滤", paramType = "query"),
        @ApiImplicitParam(name = "tableLabelName", value = "根据表名过滤", paramType = "query"),
        @ApiImplicitParam(name = "tableName", value = "根据表代码过滤", paramType = "query"),
        @ApiImplicitParam(name = "sourceType", value = "根据表类型过滤。资源类型,D:关系数据库 M:MongoDb R:redis E:elssearch K:kafka B:rabbitmq,H http服务", paramType = "query")
    })
    @GetMapping(value = "/listCombineTables")
    @WrapUpResponseBody
    public PageQueryResult listCombineTables(PageDesc pageDesc, HttpServletRequest request) {
        Map<String, Object> parameters = collectRequestParameters(request);
        String topUnit = WebOptUtils.getCurrentTopUnit(request);
        parameters.put("topUnit", topUnit);
        List list = metaTableManager.listCombineTablesByProperty(parameters, pageDesc);
        tableDictionaryMap(list);
        return PageQueryResult.createResult(list, pageDesc);
    }

    @ApiOperation(value = "同步数据库")
    @ApiImplicitParam(name = "databaseCode", value = "数据库ID")
    @GetMapping(value = "/sync/{databaseCode}")
    @WrapUpResponseBody
    public ResponseData syncDb(@PathVariable String databaseCode, String[] tableNames, HttpServletRequest request) {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        if (tableNames == null) {
            metaTableManager.syncDb(databaseCode, userCode, null);
        } else {
            metaTableManager.syncDb(databaseCode, userCode, tableNames);
        }
        return ResponseData.makeSuccessResponse();
    }

    private List<SimpleTableInfo> fetchPdmTables(String tempFilePath) {
        if ("".equals(tempFilePath)) {
            throw new ObjectException("pdm文件不能为空");
        }
        return PdmTableInfoUtils.importTableFromPdm(tempFilePath);
    }

    /**
     * 对listCombineTables中的表数据集合进行字段翻译
     *
     * @param list
     */
    private void tableDictionaryMap(List list) {
        DictionaryMapColumn dicMap1 = new DictionaryMapColumn("recorder", "recorderName", "userCode");
        DictionaryMapColumn dicMap2 = new DictionaryMapColumn("tableType", "tableTypeText", "tableType");
        ArrayList<DictionaryMapColumn> dicMaps = new ArrayList<>();
        dicMaps.add(dicMap1);
        dicMaps.add(dicMap2);
        DictionaryMapUtils.mapJsonArray(list, dicMaps);
    }

    /**
     * 转换发布结果响应体
     *
     * @param ret
     * @return
     */
    private JSONObject translatePublishMessage(Pair<Integer, String> ret) {
        JSONObject json = new JSONObject();
        json.put(ResponseData.RES_CODE_FILED, ret.getLeft());
        if (ret.getLeft() == 1) {
            json.put(ResponseData.RES_MSG_FILED, "发布失败");
        } else if (ret.getLeft() == 0) {
            json.put(ResponseData.RES_MSG_FILED, "发布成功");
        } else {
            json.put(ResponseData.RES_MSG_FILED, ret.getRight());
        }
        json.put(ResponseData.RES_DATA_FILED, ret.getRight());
        return json;
    }

    @ApiOperation(value = "分页查询数据库表数据列表")
    @RequestMapping(value = "/{databaseId}/viewlist", method = RequestMethod.POST)
    @WrapUpResponseBody
    public JSONArray viewList(@PathVariable String databaseId, @RequestBody JSONObject sql) throws IOException, SQLException {
        return metaTableManager.viewList(databaseId, sql.getString("sql"));
    }

    @ApiOperation(value = "根据中文名称翻译表名或者字段名")
    @RequestMapping(value = "/map/column", method = RequestMethod.GET)
    @WrapUpResponseBody
    public String mapLabelToColumn(@RequestParam("labelName") String labelName)  {
        return translateColumn.transLabelToColumn(labelName);
    }

    @ApiOperation(value = "根据中文名称翻译属性名称")
    @RequestMapping(value = "/map/property", method = RequestMethod.GET)
    @WrapUpResponseBody
    public String mapLabelToProperty(@RequestParam("labelName") String labelName) {
        return translateColumn.transLabelToProperty(labelName);
    }

    @ApiOperation(value = "导入TableStore中导入表结构")
    @ApiImplicitParam(name = "databaseCode", type = "path", value = "数据库ID")
    @RequestMapping(value = "/import/{databaseCode}", method = RequestMethod.POST)
    @WrapUpResponseBody
    public void importFromTableStore(@PathVariable String databaseCode, HttpServletRequest request) throws IOException{
        String userCode = WebOptUtils.getCurrentUserCode(request);
        if(StringUtils.isBlank(userCode)){
            throw new ObjectException(ResponseData.ERROR_FORBIDDEN, "获取当前用户信息失败，原因可能是用户没登录，或者session已失效！");
        }

        Pair<String, InputStream> fileInfo = UploadDownloadUtils.fetchInputStreamFromMultipartResolver(request);
        JSONObject jsonObject = JSON.parseObject(fileInfo.getRight());
        if(jsonObject!=null && //检验json的合法性
            jsonObject.containsKey("projectInfo") && jsonObject.containsKey("tables") && jsonObject.containsKey("modules")){
            metaTableManager.importFromTableStore(databaseCode, jsonObject, userCode);
        } else {
            throw new ObjectException(ObjectException.DATA_VALIDATE_ERROR, "文件中的json格式不正确！");
        }
    }

    /* --- 批量操作接口 ：
     ** 一 、设置某一个字段（字段属性名）的所有属性，包括：默认值生成规则、脱敏、校验、类型等等；
     二、统一设置（添加或者修改）字段
     三、统一删除字段
     ** 四、统一设置表的属性，比如：逻辑删除等等
     */
    @ApiOperation(value = "批量修改表的属性，只能修改 逻辑删除标识、更新版本标识 、是否写入日志、是否全文检索 这四个属性")
    @PutMapping(value = "/batchSetColumn")
    @ApiImplicitParam(name = "formJsonString", paramType="body", value = "JSON中分两部分，一部分是查询条件，一部分是修改的属性")
    @WrapUpResponseBody
    public void batchUpdateTableProps(@RequestBody String formJsonString){
        JSONObject formJson = JSONObject.parseObject(formJsonString);
        JSONObject filter = formJson.getJSONObject("filter");
        if(filter==null) return ;

        JSONObject props = formJson.getJSONObject("props");
        if(props==null || props.isEmpty()) return ;
        String columnName = props.getString("columnName");
        if(StringUtils.isNotBlank(columnName)) return;
        PendingMetaColumn columnInfo = props.toJavaObject(PendingMetaColumn.class);
        List<PendingMetaTable> tables =  metaTableManager.searchPendingMetaTable(filter);
        if(tables==null || tables.isEmpty()) return;
        for(PendingMetaTable metaTable : tables){
            metaTableManager.updatePendingMetaColumn(metaTable, columnInfo);
        }
    }

    @ApiOperation(value = "批量修改表的字段属性；可以修改 生成规则、脱敏、校验、应用、应用、延时加载 和 必填 等属性")
    @PutMapping(value = "/batchDeleteColumn")
    @WrapUpResponseBody
    public void batchUpdateTableColumns(@RequestBody String formJsonString){
        JSONObject formJson = JSONObject.parseObject(formJsonString);
        JSONObject filter = formJson.getJSONObject("filter");
        if(filter==null) return ;

        JSONObject props = formJson.getJSONObject("props");
        if(props==null || props.isEmpty()) return ;
        String columnName = props.getString("columnName");
        if(StringUtils.isNotBlank(columnName)) return;
        List<PendingMetaTable> tables =  metaTableManager.searchPendingMetaTable(filter);
        if(tables==null || tables.isEmpty()) return;
        for(PendingMetaTable metaTable : tables){
            metaTableManager.deletePendingMetaColumn(metaTable, columnName);
        }
    }
}
