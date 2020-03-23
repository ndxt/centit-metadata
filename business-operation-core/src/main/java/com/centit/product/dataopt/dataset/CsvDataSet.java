package com.centit.product.dataopt.dataset;

import com.alibaba.fastjson.JSONObject;
import com.centit.product.dataopt.core.DataSet;
import com.centit.support.algorithm.DatetimeOpt;
import com.centit.support.compiler.Pretreatment;
import com.centit.support.file.FileIOOpt;
import com.csvreader.CsvWriter;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CsvDataSet extends FileDataSet{

    /**
     * 读取 dataSet 数据集
     * @param params 模块的自定义参数
     * @return dataSet 数据集
     */
    @Override
    public DataSet load(Map<String, Object> params) {
        return null;
    }
    /**
     * 将 dataSet 数据集 持久化
     * @param dataSet 数据集
     */
    @Override
    public void save(DataSet dataSet) {
        String fileDate = DatetimeOpt.convertDateToString(DatetimeOpt.currentUtilDate(), "YYYYMMddHHmmss");
        File file = new File(filePath + File.separator  + fileDate);
        if (!file.exists()) {
            file.mkdirs();
        }
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(new File(file.getPath() + File.separator + "sys.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedWriter writer = null;
        writer = new BufferedWriter(new OutputStreamWriter(
            outputStream, Charset.forName("gbk")));
        CsvWriter csvWriter = new CsvWriter(writer, ',');
        csvWriter.setTextQualifier('"');
        csvWriter.setUseTextQualifier(true);
        csvWriter.setRecordDelimiter(IOUtils.LINE_SEPARATOR.charAt(0));
        int iHead = 0;
        for (Map<String, Object> row : dataSet.getData()) {
            if (iHead == 0) {
                try {
                    csvWriter.writeRecord(row.keySet().toArray(new String[0]));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            iHead++;
            try {
                List<String> splitedRows = new ArrayList<String>();
                for (String key : row.keySet()) {
                    Object column = row.get(key);
                    if (null != column) {
                        if (column instanceof JSONObject) {
                            File fileJSON = new File(file.getPath()
                                + File.separator + key);
                            if (!fileJSON.exists()) {
                                fileJSON.mkdirs();
                            }
                            FileIOOpt.writeObjectAsJsonToFile(row, file.getPath()
                                + File.separator + key + File.separator + Pretreatment.mapTemplateString("{modelId}", row));
                            splitedRows.add(column.toString());
                        } else {
                            splitedRows.add(column.toString());
                        }
                    } else {
                        splitedRows.add("");
                    }

                }
                csvWriter.writeRecord(splitedRows.toArray(new String[0]));

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        csvWriter.close();
        IOUtils.closeQuietly(outputStream);
    }

    /**
     * 默认和 save 等效;
     * 对于文件类持久化方案来说可以差别化处理，比如添加到文件末尾
     * @param dataSet 数据集
     */
    @Override
    public void append(DataSet dataSet) {

    }

}
