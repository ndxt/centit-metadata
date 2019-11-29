package com.centit.product.dataopt.dataset;

import com.centit.fileserver.client.DefaultFileClient;
import com.centit.product.dataopt.core.DataSetReader;
import com.centit.product.dataopt.core.DataSetWriter;

/**
 * 需要设置一个文件路径
 */
public abstract class FileDataSet implements DataSetReader, DataSetWriter {

    protected String filePath;
    public final static DefaultFileClient fileClient = new DefaultFileClient();

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public static void init(String appServerUrl) {
        fileClient.init(appServerUrl,appServerUrl,"u0000000", "000000",appServerUrl);
    }
}
