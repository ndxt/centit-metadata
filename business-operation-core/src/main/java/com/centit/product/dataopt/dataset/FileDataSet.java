package com.centit.product.dataopt.dataset;

import com.centit.product.dataopt.core.DataSetWriter;
import com.centit.product.dataopt.core.DataSetReader;

/**
 * 需要设置一个文件路径
 */
public abstract class FileDataSet implements DataSetReader, DataSetWriter {

    protected String filePath;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
