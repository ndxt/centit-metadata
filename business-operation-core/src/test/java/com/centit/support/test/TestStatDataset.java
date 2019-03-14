package com.centit.support.test;

import com.centit.support.dataopt.core.DataSet;
import com.centit.support.dataopt.core.SimpleDataSet;
import com.centit.support.dataopt.utils.DataSetOptUtil;
import junit.framework.TestCase;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;
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
 * DataSetOptUtil.statDataset单元测试
 *
 * @author chen_l
 * @date 2019/3/8
 */
public class TestStatDataset extends TestCase {
    //打印日志
    Logger logger = LoggerFactory.getLogger(TestCrossTabulation.class);
    //测试excel存放路径
    private final String path = Class.class.getClass().getResource("/").getPath() + "com\\centit\\support\\test\\TestStatDataset.xlsx";
    //输入数据集的数据存放sheet页名称
    private final String initDataSheetName = "initData";
    //分组（排序）字段、统计字段的数据存放sheet页名称
    private final String groupFieldsSheetName = "GroupFields";
    // 预期结果数据集的数据存放sheet页名称
    private final String resultSheetName = "result";
    //分组（排序）字段数据的行头名称
    private final String groupFieldsName = "groupFields";
    //统计字段数据的行头名称
    private final String statDescName = "statDesc";
    //输入数据集的数据集合
    private List<Map<String, Object>> initDataMapList = new ArrayList<Map<String, Object>>();
    //分组（排序）字段的数据集合
    private List<String> groupFields = new ArrayList<String>();
    //统计字段的数据集合
    private List<Triple<String, String, String>> statDescList = new ArrayList<Triple<String, String, String>>();
    //预期结果数据集的数据集合
    private List<Map<String, Object>> resultMapList = new ArrayList<Map<String, Object>>();


    @Before
    public void setUp() throws Exception {
        super.setUp();
        Map<String, List<List<Object>>> dataMap = new HashMap<String, List<List<Object>>>();
        ReadExcel readExcel = new ReadExcel();
        dataMap = readExcel.getDataMap(path);
        initDataMapList = readExcel.getDataMapList(dataMap.get(initDataSheetName), true);
        Map<String, ArrayList<String>> fieldsMap = readExcel.getFieldsMap(dataMap.get(groupFieldsSheetName));
        groupFields = fieldsMap.get(groupFieldsName);
        for (String desc : fieldsMap.get(statDescName)) {
            String[] descArray = desc.split(":");
            statDescList.add(new MutableTriple<String, String, String>(descArray[0], descArray[1], descArray[2]));
        }
        resultMapList = readExcel.getDataMapList(dataMap.get(resultSheetName), true);
    }

    @Test
    public void testStatDataset() {
        DataSet resultSetActual = DataSetOptUtil.statDataset(new SimpleDataSet(initDataMapList), groupFields, statDescList);
        System.out.println("预期结果：" + resultMapList);
        System.out.println("实际结果：" + resultSetActual.getData());
        Assert.assertEquals(resultMapList, resultSetActual.getData());
        System.out.println("方法statDataset测试通过！");
        logger.info("方法statDataset测试通过！");
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }


}
