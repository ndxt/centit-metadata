package com.centit.product.dataopt.bizopt;

import com.centit.fileserver.client.ClientAsFileStore;
import com.centit.fileserver.client.FileClientImpl;
import com.centit.product.dataopt.dataset.ExcelDataSet;
import com.centit.product.dataopt.dataset.FileDataSet;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class FileTest {
    public static void main(String[] args) throws Exception {
        FileClientImpl fileClient = new FileClientImpl();
        String appServerUrl = "http://localhost:8084/fileserver/fileserver";
        fileClient.init(appServerUrl,appServerUrl,"u0000000", "000000",appServerUrl);
        ClientAsFileStore fileStore = new ClientAsFileStore();
        fileStore.setFileClient(fileClient);
        //testGetAccessToken();
        FileDataSet excelDataSet = new ExcelDataSet();
        String fileId="0350daaac03e4867b0ee88527bc5e6d4";
        File excelFile = fileStore.getFile(fileId);
        Map<String, Object> params = new HashMap<>();
        params.put("FileId",fileId);
        excelDataSet.setFilePath(excelFile.getPath());
        excelDataSet.load(params);
        //testDownloadFileInfo();
    }
    @Test
    public void ss(){

    }
}
