package com.centit.product.dataopt.dataset;

import com.centit.product.dataopt.core.DataSet;
import com.centit.product.dataopt.core.SimpleDataSet;
import com.centit.support.report.ExcelImportUtil;
import com.centit.support.report.ExcelTypeEnum;
import java.util.Map;

public class ExcelDataSet extends FileDataSet {

    @Override
    public SimpleDataSet load(Map<String, Object> params) {
        try {
            SimpleDataSet dataSet = new SimpleDataSet();
            if (ExcelTypeEnum.checkFileExcelType(getFilePath())!=ExcelTypeEnum.NOTEXCEL)
               dataSet.setData(ExcelImportUtil.loadMapFromExcelSheet(getFilePath(), 0));
            return dataSet;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取字段信息
     * @return
     */
    public String[] getColumns(){
    try {
        if (ExcelTypeEnum.checkFileExcelType(getFilePath())!=ExcelTypeEnum.NOTEXCEL)
            return  ExcelImportUtil.loadColumnsFromExcel(getFilePath(), 0);
    } catch (Exception e) {
        e.printStackTrace();
    }
    return null;
}
    @Override
    public void save(DataSet dataSet) {

    }
}
