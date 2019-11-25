package com.centit.product.dataopt.dataset;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.fileserver.client.DefaultFileClient;
import com.centit.framework.appclient.AppSession;
import com.centit.product.dataopt.core.DataSet;
import com.centit.product.dataopt.core.SimpleDataSet;
import com.centit.support.algorithm.CollectionsOpt;
import com.centit.support.json.JSONOpt;
import com.centit.support.report.ExcelImportUtil;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ExcelDataSet extends FileDataSet {

    @Override
    public SimpleDataSet load(Map<String, Object> params) {
        try {
            if (!new File(filePath).exists()) {
                fileClient.downloadFile((String)params.get("FileId"), filePath);
            }
            SimpleDataSet dataSet = new SimpleDataSet();
            dataSet.setData(ExcelImportUtil.loadMapFromExcelSheet(filePath, 0));
            return dataSet;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void save(DataSet dataSet) {

    }
}
