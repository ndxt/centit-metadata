package com.centit.product.dbdesign.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.fileserver.utils.SystemTempFileUtils;
import com.centit.fileserver.utils.UploadDownloadUtils;
import com.centit.framework.common.JsonResultUtils;
import com.centit.framework.common.ResponseData;
import com.centit.framework.common.ResponseMapData;
import com.centit.framework.common.WebOptUtils;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpContentType;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.product.dbdesign.dao.PendingMetaTableDao;
import com.centit.product.dbdesign.pdmutils.PdmTableInfoUtils;
import com.centit.product.dbdesign.po.MetaChangLog;
import com.centit.product.dbdesign.po.PendingMetaColumn;
import com.centit.product.dbdesign.po.PendingMetaTable;
import com.centit.product.dbdesign.service.MetaChangLogManager;
import com.centit.product.dbdesign.service.MetaTableManager;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaTable;
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
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
    private MetaTableManager mdTableMag;

    @Resource
    private MetaChangLogManager mdChangLogMag;

    @Resource
    private PendingMetaTableDao pendingMetaTableDao;

    @ApiOperation(value = "查询重构记录")
    @RequestMapping(value = "/{databaseCode}/log", method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult loglist(@PathVariable String databaseCode, String[] field, PageDesc pageDesc,
                                   HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> searchColumn = collectRequestParameters(request);
        searchColumn.put("databaseCode",databaseCode);
        JSONArray listObjects = mdChangLogMag.listMdChangLogsAsJson(field, searchColumn, pageDesc);
        if (ArrayUtils.isNotEmpty(field)) {
            return PageQueryResult.createJSONArrayResult(listObjects, pageDesc, field, MetaChangLog.class);
        }
        else{
            return PageQueryResult.createJSONArrayResult(listObjects,pageDesc,MetaChangLog.class);
        }
    }

    @ApiOperation(value = "查看单个表重构记录")
    @RequestMapping(value = "/log/{changeId}", method = {RequestMethod.GET})
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public MetaChangLog getMdTableLog(@PathVariable String changeId) {
        MetaChangLog changLog = mdTableMag.getMetaChangLog(changeId);
        return changLog;
    }

    @ApiOperation(value = "查询重构表")
    @ApiImplicitParam(name = "databaseCode", value = "数据库代码")
    @RequestMapping(value="/{databaseCode}/list",method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult listdraft(@PathVariable String databaseCode, String[] field, PageDesc pageDesc,
                                     HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> searchColumn = collectRequestParameters(request);
        searchColumn.put("databaseCode",databaseCode);
        JSONArray listObjects = mdTableMag.listDrafts(field, searchColumn, pageDesc);
        if (ArrayUtils.isNotEmpty(field)) {
            return PageQueryResult.createJSONArrayResult(listObjects, pageDesc, field, PendingMetaTable.class);
        }
        else{
            return PageQueryResult.createJSONArrayResult(listObjects,pageDesc,PendingMetaTable.class);
        }
    }

    @ApiOperation(value = "查询单个表重构字段")
    @RequestMapping(value = "/{tableId}", method = {RequestMethod.GET})
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public PendingMetaTable getMdTableDraft(@PathVariable String tableId,HttpServletRequest request) {
        PendingMetaTable mdTable = mdTableMag.getPendingMetaTable(tableId);
        if (null !=mdTable){
            if (null ==mdTable.getColumns()){
                mdTable.setMdColumns(new ArrayList<>());
            }
            return mdTable;
        }
        //如果mdTable数据为空，重新用MetaTable数据初始化
        MetaTable metaTable = mdTableMag.getMetaTableWithReferences(tableId);
        if (null == metaTable){
            throw new ObjectException("tableId有误!");
        }
        mdTable = new PendingMetaTable();
        BeanUtils.copyProperties(metaTable,mdTable);
        String userCode = WebOptUtils.getCurrentUserCode(request);
        mdTable.setRecorder(userCode);
        mdTable.setTableState("W");
        List<PendingMetaColumn> mdColumns = new ArrayList<>();
        List<MetaColumn> metaColumns = metaTable.getColumns();
        for (MetaColumn metaColumn : metaColumns) {
            PendingMetaColumn pendingMetaColumn = new PendingMetaColumn();
            BeanUtils.copyProperties(metaColumn,pendingMetaColumn);
            mdColumns.add(pendingMetaColumn);
        }
        mdTable.setMdColumns(mdColumns);
        mdTableMag.saveNewPendingMetaTable(mdTable);
        return mdTable;
    }

    @ApiOperation(value = "新增重构表")
    @RequestMapping(method = {RequestMethod.POST})
    public void createMdTable(PendingMetaTable mdTable,HttpServletRequest request,
                              HttpServletResponse response) {
        Map<String, Object> searchColumn = new HashMap<>();
        searchColumn.put("tableName",mdTable.getTableName());
        searchColumn.put("databaseCode",mdTable.getDatabaseCode());
        PageDesc pageDesc=new PageDesc();
        JSONArray listObjects = mdTableMag.listDrafts(new String[]{}, searchColumn, pageDesc);
        if (listObjects.isEmpty()) {
            String userCode = WebOptUtils.getCurrentUserCode(request);
            mdTable.setRecorder(userCode);
            mdTable.setTableState("W");
            PendingMetaTable table = new PendingMetaTable();
            table.copyNotNullProperty(mdTable);
            /*if("C".equals(table.getTableType())){
                PendingMetaColumn col = table.findFieldByName(MetaTable.OBJECT_AS_CLOB_ID_PROP);
                if (col == null) {
                    col = new PendingMetaColumn(table, MetaTable.OBJECT_AS_CLOB_ID_FIELD);
                    col.setFieldLabelName("大字段ID");
                    col.setColumnComment("大字段ID");
                    col.setFieldType(FieldType.STRING);
                    col.setMaxLength(64);
                    col.setPrimaryKey(true);
                    col.setLastModifyDate(DatetimeOpt.currentUtilDate());
                    col.setRecorder(userCode);
                    table.addMdColumn(col);
                }
                col = table.findFieldByName(MetaTable.OBJECT_AS_CLOB_PROP);
                if (col == null) {
                    col = new PendingMetaColumn(table, MetaTable.OBJECT_AS_CLOB_FIELD);
                    col.setFieldLabelName("大字段field");
                    col.setColumnComment("大字段field");
                    col.setFieldType(FieldType.JSON_OBJECT);
                    col.setLastModifyDate(DatetimeOpt.currentUtilDate());
                    col.setRecorder(userCode);
                    table.addMdColumn(col);
                }
            }*/
            mdTableMag.saveNewPendingMetaTable(table);
            JsonResultUtils.writeSingleDataJson(table.getTableId(), response);
        } else{
            JsonResultUtils.writeErrorMessageJson(800,mdTable.getTableName()+"已存在", response);
        }
    }

    @ApiOperation(value = "修改重构表")
    @PutMapping(value = "/table/{tableId}")
    @WrapUpResponseBody
    public void updateMetaTable(@PathVariable String tableId, @RequestBody PendingMetaTable metaTable,HttpServletRequest request){
        String userCode = WebOptUtils.getCurrentUserCode(request);
        metaTable.setTableId(tableId);
        metaTable.setRecorder(userCode);
        mdTableMag.updateMetaTable(metaTable);
    }

    @ApiOperation(value = "修改重构表字段")
    @PutMapping(value = "/column/{tableId}/{columnCode}")
    @WrapUpResponseBody
    public void updateMetaColumns(@PathVariable String tableId, @PathVariable String columnCode, @RequestBody PendingMetaColumn metaColumn){
        metaColumn.setTableId(tableId);
        metaColumn.setColumnName(columnCode);
        mdTableMag.updateMetaColumn(metaColumn);
    }

    @ApiOperation(value = "编辑重构表")
    @RequestMapping(value = "/{tableId}", method = {RequestMethod.PUT})
    @WrapUpResponseBody
    public void updateMdTable(@PathVariable String tableId, @RequestBody PendingMetaTable mdTable) {
        mdTable.setTableId(tableId);
        List<String> sqls = mdTableMag.makeAlterTableSqls(mdTable);
        if(sqls.size() > 0){
            mdTable.setTableState("W");
        }
        mdTableMag.savePendingMetaTable(mdTable);
    }

    @ApiOperation(value = "删除重构表")
    @RequestMapping(value = "/{tableId}", method = {RequestMethod.DELETE})
    @WrapUpResponseBody
    public void deleteMdTable(@PathVariable String tableId) {
        mdTableMag.deletePendingMetaTable(tableId);
    }

    @ApiOperation(value = "查看发布重构表sql")
    @RequestMapping(value = "/beforePublish/{ptableId}", method = {RequestMethod.POST})
    @WrapUpResponseBody
    public ResponseData alertSqlBeforePublish(@PathVariable String ptableId,
                                      HttpServletRequest request, HttpServletResponse response) {
        return mdTableMag.makeAlterTableSqls(ptableId);
    }

    @ApiOperation(value = "发布重构表")
    @RequestMapping(value = "/publish/{ptableId}", method = {RequestMethod.POST})
    public void publishMdTable(@PathVariable String ptableId,
                               HttpServletRequest request, HttpServletResponse response) {
        String userCode = WebOptUtils.getCurrentUserCode(request);


        Pair<Integer, String> ret = mdTableMag.publishMetaTable(ptableId, userCode);
        JSONObject json = new JSONObject();
        json.put(ResponseData.RES_CODE_FILED, ret.getLeft());
        if (ret.getLeft() == 1){
            json.put(ResponseData.RES_MSG_FILED, "发布失败");
        } else if (ret.getLeft() == 0) {
            json.put(ResponseData.RES_MSG_FILED, "发布成功");
        } else
            json.put(ResponseData.RES_MSG_FILED, ret.getRight());
        json.put(ResponseData.RES_DATA_FILED, ret.getRight());
        JsonResultUtils.writeSingleDataJson(json,response);
    }

    @ApiOperation(value = "列出未加入表单的field")
    @RequestMapping(value = "/{tableId}/getField", method = RequestMethod.GET)
    public void listfield(@PathVariable String tableId, HttpServletResponse response, PageDesc pageDesc) {

        List<MetaColumn> meTadColumns =
            mdTableMag.listFields(tableId);
        ResponseMapData resData = new ResponseMapData();
        resData.addResponseData(OBJLIST, meTadColumns);
        resData.addResponseData(PAGE_DESC, pageDesc);
        JsonResultUtils.writeSingleDataJson(meTadColumns, response);

    }

    /**
     * 获取草稿序列中的tableId
     * @param response HttpServletResponse
     */
    @ApiOperation(value = "获取草稿序列中的tableId")
    @RequestMapping(value = "/draft/getNextKey", method = RequestMethod.GET)
    public void getPdNextKey(HttpServletResponse response) {

        ResponseMapData resData = new ResponseMapData();
        resData.addResponseData("tableId", pendingMetaTableDao.getNextKey());
        JsonResultUtils.writeSingleDataJson(resData, response);

    }


   private  List<SimpleTableInfo> fetchPdmTables(String tempFilePath) {
       if ("".equals(tempFilePath)) {
           throw new ObjectException("pdm文件不能为空");
       }
       return PdmTableInfoUtils.importTableFromPdm(tempFilePath);
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
        data.put("tempFilePath", token +"_"+size);
        JSONObject jsonObject = UploadDownloadUtils.makeRangeUploadJson(tempFileSize, token, token +"_"+size);
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
        try (FileOutputStream out = new FileOutputStream(new File(tempFilePath))){
            long uploadSize = FileIOOpt.writeInputStreamToOutputStream( fileInfo.getRight(), out);
            if(uploadSize>0){
                //上传到临时区成功
                JSONObject jsonObject = new JSONObject();
                Map<String, Object> data = new HashMap<>(4);
                data.put("tempFilePath", token +"_"+size);
                data.put("tables", PdmTableInfoUtils.getTableNameFromPdm(tempFilePath));
                jsonObject.put("tables", data);
                JsonResultUtils.writeSingleDataJson(jsonObject,response);
            }else {
                JsonResultUtils.writeOriginalJson(UploadDownloadUtils.
                    makeRangeUploadJson(uploadSize, token, token +"_"+size).toJSONString(), response);
            }

        }catch (ObjectException e){
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
        String tempFilePath =SystemTempFileUtils.getTempDirectory() + object.getString("tempFilePath") + ".tmp";
        JSONArray jsonArray = object.getJSONArray("data");
        List<String>  tables = new ArrayList<>();
        for (Object o : jsonArray) {
            tables.add(o.toString());
        }
        String userCode = WebOptUtils.getCurrentUserCode(request);
        Pair<Integer, String> ret = mdTableMag.syncPdm(databaseCode,tempFilePath,tables,userCode);
        JsonResultUtils.writeErrorMessageJson(ret.getLeft(), ret.getRight(), response);
    }

    @ApiOperation(value = "批量发布表元数据表")
    @RequestMapping(value = "/{databaseCode}/publish", method = {RequestMethod.POST})
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public void publishDatabase(@PathVariable String databaseCode,
                               HttpServletRequest request, HttpServletResponse response) {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        Pair<Integer, String> ret = mdTableMag.publishDatabase(databaseCode, userCode);
        JSONObject json = new JSONObject();
        json.put(ResponseData.RES_CODE_FILED, ret.getLeft());
        if (ret.getLeft() == 1){
            json.put(ResponseData.RES_MSG_FILED, "发布失败");
        } else if (ret.getLeft() == 0) {
            json.put(ResponseData.RES_MSG_FILED, "发布成功");
        } else
            json.put(ResponseData.RES_MSG_FILED, ret.getRight());
        json.put(ResponseData.RES_DATA_FILED, ret.getRight());
        JsonResultUtils.writeSingleDataJson(json,response);
    }

    @ApiOperation(value = "查询列表元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表ID")
    })
    @GetMapping(value = "/{tableId}/columns")
    @WrapUpResponseBody
    public PageQueryResult<PendingMetaColumn> listColumns(@PathVariable String tableId, PageDesc pageDesc){
        List<PendingMetaColumn> list = mdTableMag.listMetaColumns(tableId, pageDesc);
        return PageQueryResult.createResult(list, pageDesc);
    }

    @ApiOperation(value = "查询单个列表元数据")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表元数据ID"),
        @ApiImplicitParam(name = "columnName", value = "列名")
    })
    @GetMapping(value = "/{tableId}/column/{columnName}")
    @WrapUpResponseBody(contentType = WrapUpContentType.MAP_DICT)
    public PendingMetaColumn getColumn(@PathVariable String tableId, @PathVariable String columnName){
        return mdTableMag.getMetaColumn(tableId, columnName);
    }


    @ApiOperation(value = "查询列元数据,pending表与md表数据的组合")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "tableId", value = "表ID")
    })
    @GetMapping(value = "/{databaseCode}/listCombineTables")
    @WrapUpResponseBody
    public PageQueryResult listCombineTables(@PathVariable String databaseCode, PageDesc pageDesc ,HttpServletRequest request) {
        Map<String, Object> parameters = collectRequestParameters(request);
        List list = mdTableMag.listCombineTables(parameters, pageDesc);
        return PageQueryResult.createResult(list, pageDesc);
    }


    @ApiOperation(value = "查询列元数据,pending表与md表数据的组合,通过osId或optId过滤")
    @ApiImplicitParams(value = {
        @ApiImplicitParam(name = "osId", value = "应用id"),
        @ApiImplicitParam(name = "optId", value = "操作id")
    })
    @GetMapping(value = "/listCombineTables")
    @WrapUpResponseBody
    public PageQueryResult listCombineTables( PageDesc pageDesc ,HttpServletRequest request) {
        Map<String, Object> parameters = collectRequestParameters(request);
        List list = mdTableMag.listCombineTablesByProperty(parameters, pageDesc);
        return PageQueryResult.createResult(list, pageDesc);
    }
}
