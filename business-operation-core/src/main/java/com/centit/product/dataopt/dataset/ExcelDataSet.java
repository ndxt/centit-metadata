package com.centit.product.dataopt.dataset;

import com.centit.product.dataopt.core.DataSet;
import com.centit.product.dataopt.core.SimpleDataSet;
import com.centit.support.report.ExcelImportUtil;
import com.centit.support.report.ExcelTypeEnum;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ExcelDataSet extends FileDataSet {

    @Override
    public SimpleDataSet load(Map<String, Object> params) {
        try {
            checkFile(params);
            SimpleDataSet dataSet = new SimpleDataSet();
            if (ExcelTypeEnum.checkFileExcelType(filePath)!=ExcelTypeEnum.NOTEXCEL)
               dataSet.setData(ExcelImportUtil.loadMapFromExcelSheet(filePath, 0));
            return dataSet;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void checkFile(Map<String, Object> params) throws IOException {
        setFilePath(System.getProperty("java.io.tmpdir")+params.get("FileId")+".tmp");
        if (!new File(filePath).exists()||new File(filePath).length()==0) {
            fileClient.downloadFile((String)params.get("FileId"), filePath);
        }
    }

    public String[] getColumns(Map<String, Object> params){
    try {
        checkFile(params);
        if (ExcelTypeEnum.checkFileExcelType(filePath)!=ExcelTypeEnum.NOTEXCEL)
            return  ExcelImportUtil.loadColumnsFromExcel(filePath, 0);
    } catch (Exception e) {
        e.printStackTrace();
    }
    return null;
}
    @Override
    public void save(DataSet dataSet) {

    }
}
