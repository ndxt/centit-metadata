package com.centit.product.test;

import com.centit.product.dataopt.core.DataSet;
import com.centit.product.dataopt.core.SimpleDataSet;
import com.centit.product.dataopt.utils.DataSetOptUtil;
import junit.framework.TestCase;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * DataSetOptUtil.analyseDataset单元测试
 *
 * @author chen_l
 * @date 2019/3/11
 */
public class TestAnalyseDataset extends TestCase {
    //打印日志
    Logger logger = LoggerFactory.getLogger(TestCrossTabulation.class);
    //测试excel存放路径
    private final String path = Class.class.getClass().getResource("/").getPath() + "com/centit/product/test/TestAnalyseDataset.xlsx";
    //输入数据集的数据存放sheet页名称
    private final String initDataSheetName = "initData";
    //分组（排序）字段、统计字段的数据存放sheet页名称
    private final String fieldsSheetName = "Fields";
    // 预期结果数据集的数据存放sheet页名称
    private final String resultSheetName = "result";
    //分组字段数据的行头名称
    private final String groupFieldsName = "groupbyFields";
    //排序字段数据的行头名称
    private final String orderbyFieldsName = "orderbyFields";
    //引用说明数据的行头名称
    private final String refDescName = "refDesc";
    //输入数据集的数据集合
    private List<Map<String, Object>> initDataMapList = new ArrayList<Map<String, Object>>();
    //分组字段的数据集合
    private List<String> groupFields = new ArrayList<String>();
    //排序字段的数据集合
    private List<String> orderbyFields = new ArrayList<String>();
    //引用说明的数据集合
    private List<Pair<String, String>> refDescList = new ArrayList<Pair<String, String>>();
    //预期结果数据集的数据集合
    private List<Map<String, Object>> resultMapList = new ArrayList<Map<String, Object>>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Map<String, List<List<Object>>> dataMap = new HashMap<String, List<List<Object>>>();
        ReadExcel readExcel = new ReadExcel();
        dataMap = readExcel.getDataMap(path);
        initDataMapList = readExcel.getDataMapList(dataMap.get(initDataSheetName), true);
        Map<String, ArrayList<String>> fieldsMap = readExcel.getFieldsMap(dataMap.get(fieldsSheetName));
        groupFields = fieldsMap.get(groupFieldsName);
        orderbyFields = fieldsMap.get(orderbyFieldsName);
        for (String desc : fieldsMap.get(refDescName)) {
            String[] descArray = desc.split(":");
            refDescList.add(new MutablePair<String, String>(descArray[0], descArray[1]));
        }
        resultMapList = readExcel.getDataMapList(dataMap.get(resultSheetName), true);
    }

    @Test
    public void testStatDataset() {
        DataSet resultSetActual = DataSetOptUtil.analyseDataset(new SimpleDataSet(initDataMapList), groupFields, orderbyFields, (Collection)refDescList);
        System.out.println("预期结果：" + resultMapList);
        System.out.println("实际结果：" + resultSetActual.getData());
        Assert.assertEquals(resultMapList, resultSetActual.getData());
        System.out.println("方法analyseDataset测试通过！");
        logger.info("方法analyseDataset测试通过！");
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }


}
