package com.centit.support.test;

import com.centit.support.dataopt.core.DataSet;
import com.centit.support.dataopt.core.SimpleDataSet;
import com.centit.support.dataopt.utils.DataSetOptUtil;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DataSetOptUtil.crossTabulation单元测试
 *
 * @author chen_l
 * @date 2019/3/7
 */
public class TestCrossTabulation extends TestCase {
    //打印日志
    Logger logger = LoggerFactory.getLogger(TestCrossTabulation.class);
    //测试excel存放路径
    private final String path = Class.class.getClass().getResource("/").getPath() + "com/centit/support/test/TestCrossTabulation.xlsx";
    //输入数据集的数据存放sheet页名称
    private final String initDataSheetName = "initData";
    //行头、列头信息的数据存放sheet页名称
    private final String HeaderFieldsSheetName = "HeaderFields";
    // 预期结果数据集的数据存放sheet页名称
    private final String resultSheetName = "result";
    //行头数据的行头名称
    private final String rowHeaderFieldsName = "rowHeaderFields";
    //列头数据的行头名称
    private final String colHeaderFieldsName = "colHeaderFields";
    //输入数据集的数据集合
    private List<Map<String, Object>> initDataMapList = new ArrayList<Map<String, Object>>();
    //行头数据的数据集合
    private List<String> rowHeaderFields = new ArrayList<String>();
    //列头数据的数据集合
    private List<String> colHeaderFields = new ArrayList<String>();
    //预期结果数据集的数据集合
    private List<Map<String, Object>> resultMapList = new ArrayList<Map<String, Object>>();


    @Before
    public void setUp() throws Exception {
        super.setUp();
        Map<String, List<List<Object>>> dataMap = new HashMap<String, List<List<Object>>>();
        ReadExcel readExcel = new ReadExcel();
        dataMap = readExcel.getDataMap(path);
        initDataMapList = readExcel.getDataMapList(dataMap.get(initDataSheetName), true);
        Map<String, ArrayList<String>> fieldsMap = readExcel.getFieldsMap(dataMap.get(HeaderFieldsSheetName));
        rowHeaderFields = fieldsMap.get(rowHeaderFieldsName);
        colHeaderFields = fieldsMap.get(colHeaderFieldsName);
        resultMapList = readExcel.getDataMapList(dataMap.get(resultSheetName), false);
    }

    @Test
    public void testCrossTabulation() {
        DataSet resultSetActual = DataSetOptUtil.crossTabulation(new SimpleDataSet(initDataMapList), rowHeaderFields, colHeaderFields);
        System.out.println("预期结果：" + resultMapList);
        System.out.println("实际结果：" + resultSetActual.getData());
        Assert.assertEquals(resultMapList, resultSetActual.getData());
        System.out.println("方法crossTabulation测试通过！");
        logger.info("方法crossTabulation测试通过！");
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }
}
