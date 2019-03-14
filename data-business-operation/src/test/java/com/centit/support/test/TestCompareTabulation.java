package com.centit.support.test;

import com.centit.support.dataopt.core.DataSet;
import com.centit.support.dataopt.core.SimpleDataSet;
import com.centit.support.dataopt.utils.DataSetOptUtil;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * DataSetOptUtil.compareTabulation单元测试
 *
 * @author chen_l
 * @date 2019/3/11
 */
public class TestCompareTabulation extends TestCase {
    //打印日志
    Logger logger = LoggerFactory.getLogger(TestCrossTabulation.class);
    //测试excel存放路径
    private final String path = Class.class.getClass().getResource("/").getPath() + "com/centit/support/test/TestCompareTabulation.xlsx";
    //本期数据集的数据存放sheet页名称
    private final String currDataSheetName = "currData";
    //上期数据集的数据存放sheet页名称
    private final String lastDataSheetName = "lastData";
    //主键列的数据存放sheet页名称
    private final String primaryFieldsSheetName = "primaryFields";
    //预期结果数据集的数据存放sheet页名称
    private final String resultSheetName = "result";
    //主键列数据的行头名称
    private final String primaryFieldsName = "primaryFields";
    //本期数据集的数据集合
    private List<Map<String, Object>> currDataMapList = new ArrayList<Map<String, Object>>();
    //上期数据集的数据集合
    private List<Map<String, Object>> lastDataMapList = new ArrayList<Map<String, Object>>();
    //主键列的数据集合
    private List<String> primaryFields = new ArrayList<String>();
    //预期结果数据集的数据集合
    private List<Map<String, Object>> resultMapList = new ArrayList<Map<String, Object>>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ReadExcel readExcel = new ReadExcel();
        Map<String, List<List<Object>>> dataMap = readExcel.getDataMap(path);
        currDataMapList = readExcel.getDataMapList(dataMap.get(currDataSheetName), true);
        lastDataMapList = readExcel.getDataMapList(dataMap.get(lastDataSheetName), true);
        primaryFields = readExcel.getFieldsMap(dataMap.get(primaryFieldsSheetName)).get(primaryFieldsName);
        resultMapList = readExcel.getDataMapList(dataMap.get(resultSheetName), true);
    }

    @Test
    public void testCompareTabulation() {
        DataSet resultSetActual = DataSetOptUtil.compareTabulation(new SimpleDataSet(currDataMapList), new SimpleDataSet(lastDataMapList), primaryFields);
        System.out.println("预期结果：" + resultMapList);
        System.out.println("实际结果：" + resultSetActual.getData());
        Assert.assertEquals(resultMapList, resultSetActual.getData());
        System.out.println("方法compareTabulationd测试通过！");
        logger.info("方法compareTabulation测试通过！");
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }


}
