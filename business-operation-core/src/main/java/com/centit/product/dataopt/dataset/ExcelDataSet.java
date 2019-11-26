package com.centit.product.dataopt.dataset;

import com.centit.product.dataopt.core.DataSet;
import com.centit.product.dataopt.core.SimpleDataSet;
import com.centit.support.report.ExcelImportUtil;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ExcelDataSet extends FileDataSet {

    @Override
    public SimpleDataSet load(Map<String, Object> params) {
        try {
            if (!new File(filePath).exists()||new File(filePath).length()==0) {
                fileClient.downloadFile((String)params.get("FileId"), filePath);
            }
            SimpleDataSet dataSet = new SimpleDataSet();
            dataSet.setData(ExcelImportUtil.loadMapFromExcelSheet(filePath, 0));
            return dataSet;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void save(DataSet dataSet) {

    }
}
