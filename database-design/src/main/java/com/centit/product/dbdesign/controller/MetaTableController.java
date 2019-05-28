package com.centit.product.dbdesign.controller;

import com.alibaba.fastjson.JSONArray;
import com.centit.framework.common.*;
import com.centit.framework.core.controller.BaseController;
import com.centit.framework.core.controller.WrapUpResponseBody;
import com.centit.framework.core.dao.PageQueryResult;
import com.centit.framework.security.model.CentitUserDetails;
import com.centit.product.dbdesign.dao.PendingMetaTableDao;
import com.centit.product.dbdesign.pdmutils.PdmTableInfo;
import com.centit.product.dbdesign.po.MetaChangLog;
import com.centit.product.dbdesign.po.PendingMetaTable;
import com.centit.product.dbdesign.service.MetaChangLogManager;
import com.centit.product.dbdesign.service.MetaTableManager;
import com.centit.product.metadata.po.MetaColumn;
import com.centit.product.metadata.po.MetaTable;
import com.centit.support.database.utils.PageDesc;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.HandlerMapping;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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

    @ApiOperation(value = "查询表元数据更改(发布)记录")
    @RequestMapping(value = "/log", method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult loglist(String[] field, PageDesc pageDesc, HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> searchColumn = convertSearchColumn(request);
        JSONArray listObjects = mdChangLogMag.listMdChangLogsAsJson(field, searchColumn, pageDesc);
        if (ArrayUtils.isNotEmpty(field)) {
            return PageQueryResult.createJSONArrayResult(listObjects, pageDesc, field, MetaChangLog.class);
        }
        else{
            return PageQueryResult.createJSONArrayResult(listObjects,pageDesc,MetaChangLog.class);
        }
    }

    @ApiOperation(value = "查询所有表元数据")
    @RequestMapping(method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult list(String[] field, PageDesc pageDesc, HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> searchColumn = convertSearchColumn(request);
        JSONArray listObjects = mdTableMag.listObjectsAsJson(searchColumn, pageDesc);
        if (ArrayUtils.isNotEmpty(field)) {
            return PageQueryResult.createJSONArrayResult(listObjects, pageDesc, field, MetaTable.class);
        }
        else{
            return PageQueryResult.createJSONArrayResult(listObjects,pageDesc,MetaTable.class);
        }
    }

    @ApiOperation(value = "获取未落实表元数据表")
    @RequestMapping(value="/listdraft",method = RequestMethod.GET)
    @WrapUpResponseBody
    public PageQueryResult listdraft(String[] field, PageDesc pageDesc, HttpServletRequest request, HttpServletResponse response) {
        Map<String, Object> searchColumn = convertSearchColumn(request);
        JSONArray listObjects = mdTableMag.listDrafts(field, searchColumn, pageDesc);
        if (ArrayUtils.isNotEmpty(field)) {
            return PageQueryResult.createJSONArrayResult(listObjects, pageDesc, field, PendingMetaTable.class);
        }
        else{
            return PageQueryResult.createJSONArrayResult(listObjects,pageDesc,PendingMetaTable.class);
        }
    }

    @ApiOperation(value = "查询单个表元数据")
    @RequestMapping(value = "/{tableId}", method = {RequestMethod.GET})
    @WrapUpResponseBody
    public MetaTable getMdTable(@PathVariable String tableId) {
        MetaTable mdTable = mdTableMag.getObjectById(tableId);
        return mdTable;
    }

    @ApiOperation(value = "查询单个表元数据表(未落实)")
    @RequestMapping(value = "/draft/{tableId}", method = {RequestMethod.GET})
    @WrapUpResponseBody
    public PendingMetaTable getMdTableDraft(@PathVariable String tableId) {
        PendingMetaTable mdTable = mdTableMag.getPendingMetaTable(tableId);
        return mdTable;
    }

    @ApiOperation(value = "新增表元数据表")
    @RequestMapping(method = {RequestMethod.POST})
    public void createMdTable(PendingMetaTable mdTable, HttpServletResponse response) {
        PendingMetaTable table = new PendingMetaTable();
        table.copyNotNullProperty(mdTable);
        if (null == table.getTableId()) {
            table.setTableId(String.valueOf(pendingMetaTableDao.getNextKey()));
        }
        mdTableMag.saveNewPendingMetaTable(table);
        JsonResultUtils.writeSingleDataJson(table.getTableId(), response);
    }

    @ApiOperation(value = "编辑表元数据表")
    @RequestMapping(value = "/draft/{tableId}", method = {RequestMethod.PUT})
    @WrapUpResponseBody
    public void updateMdTable(@PathVariable String tableId, @RequestBody PendingMetaTable mdTable) {
        mdTable.setTableId(tableId);
        mdTableMag.savePendingMetaTable(mdTable);
    }

    @ApiOperation(value = "删除单个表元数据表")
    @RequestMapping(value = "/draft/{tableId}", method = {RequestMethod.DELETE})
    @WrapUpResponseBody
    public void deleteMdTable(@PathVariable String tableId) {
        mdTableMag.deletePendingMetaTable(tableId);
    }

    @ApiOperation(value = "发布表元数据表,查看创建表的sql")
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

    @ApiOperation(value = "导入pdm修改表结构")
    @RequestMapping(value = "/pdm/{databaseCode}", method = RequestMethod.GET)
    @WrapUpResponseBody
    public void syncPdm(@PathVariable String databaseCode, String pdmFilePath,
                                    HttpServletRequest request, HttpServletResponse response) {
        String userCode = WebOptUtils.getCurrentUserCode(request);
        if(StringUtils.isBlank(userCode)){
            throw new ObjectException("未登录");
        }
        if ("".equals(pdmFilePath)) {
            throw new ObjectException("pdm文件不能为空");
        }
        pdmFilePath = StringEscapeUtils.unescapeHtml4(pdmFilePath);
        Pair<Integer, String> ret = mdTableMag.syncPdm(databaseCode,pdmFilePath,userCode);
        JsonResultUtils.writeErrorMessageJson(ret.getLeft(), ret.getRight(), response);
    }
}
