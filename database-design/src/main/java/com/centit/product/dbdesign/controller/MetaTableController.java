package com.centit.product.dbdesign.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.fileserver.utils.SystemTempFileUtils;
import com.centit.fileserver.utils.UploadDownloadUtils;
import com.centit.framework.common.*;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.product.dbdesign.dao.PendingMetaTableDao;
import com.centit.product.dbdesign.pdmutils.PdmTableInfo;
import com.centit.product.dbdesign.po.MetaChangLog;
import com.centit.product.dbdesign.po.PendingMetaTable;
import com.centit.product.dbdesign.service.MetaChangLogManager;
import com.centit.product.dbdesign.service.MetaTableManager;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.support.database.metadata.SimpleTableInfo;
import com.centit.support.database.utils.PageDesc;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
@Api(value = "表元数据", tags = "表元数据")
public class MetaTableController extends BaseController {
    //private static final Log log = LogFactory.getLog(MetaTableController.class);

    @Resource
    private MetaTableManager mdTableMag;

    @Resource
    private MetaChangLogManager mdChangLogMag;

    @Resource
    private PendingMetaTableDao pendingMetaTableDao;

    @ApiOperation(value = "查询表元数据更改发布记录")
    @RequestMapping(value = "/log", method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult loglist(String[] field, PageDesc pageDesc, HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> searchColumn = collectRequestParameters(request);
        JSONArray listObjects = mdChangLogMag.listMdChangLogsAsJson(field, searchColumn, pageDesc);
        if (ArrayUtils.isNotEmpty(field)) {
            return PageQueryResult.createJSONArrayResult(listObjects, pageDesc, field, MetaChangLog.class);
        }
        else{
            return PageQueryResult.createJSONArrayResult(listObjects,pageDesc,MetaChangLog.class);
        }
    }

    @ApiOperation(value = "查看单个表元数据发布记录")
    @RequestMapping(value = "/log/{changeId}", method = {RequestMethod.GET})
    @WrapUpResponseBody
    public MetaChangLog getMdTableLog(@PathVariable String changeId) {
        MetaChangLog changLog = mdTableMag.getMetaChangLog(changeId);
        return changLog;
    }

    @ApiOperation(value = "查询表元数据表")
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

    @ApiOperation(value = "查询单个表元数据表")
    @RequestMapping(value = "/{tableId}", method = {RequestMethod.GET})
    @WrapUpResponseBody
    public PendingMetaTable getMdTableDraft(@PathVariable String tableId) {
        PendingMetaTable mdTable = mdTableMag.getPendingMetaTable(tableId);
        return mdTable;
    }

    @ApiOperation(value = "新增表元数据表")
    @RequestMapping(method = {RequestMethod.POST})
    public void createMdTable(PendingMetaTable mdTable,HttpServletRequest request,
                              HttpServletResponse response) {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        mdTable.setRecorder(userCode);
        mdTableMag.saveNewPendingMetaTable(mdTable);
        JsonResultUtils.writeSingleDataJson(mdTable.getTableId(), response);
    }

    @ApiOperation(value = "编辑表元数据表")
    @RequestMapping(value = "/{tableId}", method = {RequestMethod.PUT})
    @WrapUpResponseBody
    public void updateMdTable(@PathVariable String tableId, @RequestBody PendingMetaTable mdTable) {
        mdTable.setTableId(tableId);
        mdTableMag.savePendingMetaTable(mdTable);
    }

    @ApiOperation(value = "删除单个表元数据表")
    @RequestMapping(value = "/{tableId}", method = {RequestMethod.DELETE})
    @WrapUpResponseBody
    public void deleteMdTable(@PathVariable String tableId) {
        mdTableMag.deletePendingMetaTable(tableId);
    }

    @ApiOperation(value = "查看发布表元数据表的sql")
    @RequestMapping(value = "/beforePublish/{ptableId}", method = {RequestMethod.POST})
    public void alertSqlBeforePublish(@PathVariable String ptableId,
                                      HttpServletRequest request, HttpServletResponse response) {
        List<String> sqls = mdTableMag.makeAlterTableSqls(ptableId);
        ResponseMapData resData = new ResponseMapData();
        resData.addResponseData(OBJLIST, sqls);
        JsonResultUtils.writeResponseDataAsJson(resData, response);
    }

    @ApiOperation(value = "发布表元数据表")
    @RequestMapping(value = "/publish/{ptableId}", method = {RequestMethod.POST})
    public void publishMdTable(@PathVariable String ptableId,
                               HttpServletRequest request, HttpServletResponse response) {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        if (StringUtils.isBlank(userCode)) {
            JsonResultUtils.writeErrorMessageJson(ResponseData.ERROR_UNAUTHORIZED,
                "当前用户没有登录，请先登录。", response);
            return;
        }
        Pair<Integer, String> ret = mdTableMag.publishMetaTable(ptableId, userCode);
        JsonResultUtils.writeErrorMessageJson(ret.getLeft(), ret.getRight(), response);
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
     *
     */
    @ApiOperation(value = "获取草稿序列中的tableId")
    @RequestMapping(value = "/draft/getNextKey", method = RequestMethod.GET)
    public void getPdNextKey(HttpServletResponse response, PageDesc pageDesc) {

        ResponseMapData resData = new ResponseMapData();
        resData.addResponseData("tableId", pendingMetaTableDao.getNextKey());
        JsonResultUtils.writeSingleDataJson(resData, response);

    }


   private  List<SimpleTableInfo> fetchPdmTables(String tempFilePath) {
       if ("".equals(tempFilePath)) {
           throw new ObjectException("pdm文件不能为空");
       }
       return PdmTableInfo.importTableFromPdm(tempFilePath);
   }

    @ApiOperation(value = "range")
    @CrossOrigin(origins = "*", allowCredentials = "true", maxAge = 86400, methods = RequestMethod.GET)
    @RequestMapping(value = "/range", method = {RequestMethod.GET})
    public void checkFileRange(String token, long size,
                               HttpServletRequest request, HttpServletResponse response)
        throws IOException {
        //FileRangeInfo fr = new FileRangeInfo(token,size);
        Pair<String, InputStream> fileInfo = UploadDownloadUtils.fetchInputStreamFromMultipartResolver(request);

        //检查临时目录中的文件大小，返回文件的其实点
        String tempFilePath = SystemTempFileUtils.getTempFilePath(token, size);
        long tempFileSize = SystemTempFileUtils.checkTempFileSize(tempFilePath);
        Map<String, Object> data = new HashMap<>(4);
        data.put("tempFilePath", token +"_"+size);
        JSONObject jsonObject = UploadDownloadUtils.makeRangeUploadJson(tempFileSize);
        if (tempFileSize == size) {
            data.put("tables",fetchPdmTables(tempFilePath));
            jsonObject.put("tables",data);
        }
        JsonResultUtils.writeSingleDataJson(jsonObject,response);
    }

    @ApiOperation(value = "导入pdm返回表数据")
    @CrossOrigin(origins = "*", allowCredentials = "true", maxAge = 86400, methods = RequestMethod.POST)
    @RequestMapping(value = "/range", method = {RequestMethod.POST})
    public void syncPdm(String token, long size,
        HttpServletRequest request, HttpServletResponse response)
        throws IOException {

        Pair<String, InputStream> fileInfo = UploadDownloadUtils.fetchInputStreamFromMultipartResolver(request);
        String tempFilePath = SystemTempFileUtils.getTempFilePath(token, size);
        try {
            long uploadSize = UploadDownloadUtils.uploadRange(tempFilePath, fileInfo.getRight(), token, size, request);
            if(uploadSize==0){
                //上传到临时区成功
                JSONObject jsonObject = new JSONObject();
                Map<String, Object> data = new HashMap<>(4);
                data.put("tempFilePath", token +"_"+size);
                data.put("tables",fetchPdmTables(tempFilePath));
                jsonObject.put("tables",data);
                JsonResultUtils.writeSingleDataJson(jsonObject,response);
                //FileSystemOpt.deleteFile(tempFilePath);
            }else if( uploadSize>0){
                JsonResultUtils.writeOriginalJson(UploadDownloadUtils.
                    makeRangeUploadJson(uploadSize).toJSONString(), response);
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
        if (StringUtils.isBlank(userCode)) {
            JsonResultUtils.writeErrorMessageJson(ResponseData.ERROR_UNAUTHORIZED,
                "当前用户没有登录，请先登录。", response);
            return;
        }
        Pair<Integer, String> ret = mdTableMag.syncPdm(databaseCode,tempFilePath,tables,userCode);
        JsonResultUtils.writeErrorMessageJson(ret.getLeft(), ret.getRight(), response);
    }

    @ApiOperation(value = "批量发布表元数据表")
    @RequestMapping(value = "/{databaseCode}/publish", method = {RequestMethod.POST})
    public void publishDatabase(@PathVariable String databaseCode,
                               HttpServletRequest request, HttpServletResponse response) {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        if (StringUtils.isBlank(userCode)) {
            JsonResultUtils.writeErrorMessageJson(ResponseData.ERROR_UNAUTHORIZED,
                "当前用户没有登录，请先登录。", response);
            return;
        }
        Pair<Integer, String> ret = mdTableMag.publishDatabase(databaseCode, userCode);
        JsonResultUtils.writeErrorMessageJson(ret.getLeft(), ret.getRight(), response);
    }
}
