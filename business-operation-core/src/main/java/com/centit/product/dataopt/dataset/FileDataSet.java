package com.centit.product.dataopt.dataset;

import com.centit.fileserver.client.DefaultFileClient;
import com.centit.framework.appclient.AppSession;
import com.centit.product.dataopt.core.DataSetReader;
import com.centit.product.dataopt.core.DataSetWriter;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.IOException;

/**
 * 需要设置一个文件路径
 */
public abstract class FileDataSet implements DataSetReader, DataSetWriter {

    private String filePath;
    private final static DefaultFileClient fileClient = new DefaultFileClient();

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public static void init(String appServerUrl) {
        fileClient.init(appServerUrl,appServerUrl,"u0000000", "000000",appServerUrl);
    }
    public static String downFile(String fileId) throws IOException {
        String filePath=System.getProperty("java.io.tmpdir")+fileId+".tmp";
        if (!new File(filePath).exists()||new File(filePath).length()==0) {
            fileClient.downloadFile(fileId, filePath);
        }
        return filePath;
    }
}
