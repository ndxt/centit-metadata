package com.centit.product.dataopt.bizopt;

import com.centit.product.dataopt.dataset.ExcelDataSet;
import com.centit.product.dataopt.dataset.FileDataSet;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class FileTest {
    public static void main(String[] args) throws Exception {
        FileDataSet.init("http://localhost:8084/fileserver/fileserver");
        //testGetAccessToken();
        FileDataSet excelDataSet=new ExcelDataSet();
        String fileId="777cfec005394bcf97f5877f59933896";
        Map<String, Object> params = new HashMap<>();
        params.put("FileId",fileId);
        excelDataSet .setFilePath(System.getProperty("java.io.tmpdir")+fileId+".tmp");
        excelDataSet.load(params);
        //testDownloadFileInfo();
    }
    @Test
    public void ss(){

    }
}
