package com.centit.product.dataopt.dataset;

import com.centit.fileserver.client.DefaultFileClient;
import com.centit.framework.appclient.AppSession;
import com.centit.product.dataopt.core.DataSetReader;
import com.centit.product.dataopt.core.DataSetWriter;
import org.springframework.context.annotation.Bean;

/**
 * 需要设置一个文件路径
 */
public abstract class FileDataSet implements DataSetReader, DataSetWriter {

    protected String filePath;
    public static DefaultFileClient fileClient;
    private static AppSession appSession;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public static void init(String appServerUrl) {
        appSession = new AppSession(appServerUrl, false, "u0000000", "000000");
        fileClient = new DefaultFileClient();
        fileClient.setAppSession(appSession);
        fileClient.setFileServerExportUrl(appServerUrl);
    }
}
