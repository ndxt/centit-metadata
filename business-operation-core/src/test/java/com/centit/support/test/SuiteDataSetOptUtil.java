package com.centit.support.test;

import junit.framework.TestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * 测试套，包含各个方法的单元测试，可以一键测试
 *
 * @author chen_l
 * @date 2019/3/11
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({TestCrossTabulation.class, TestStatDataset.class, TestCompareTabulation.class, TestAnalyseDataset.class})
public class SuiteDataSetOptUtil extends TestSuite {

}
