package com.centit.product.metadata.dao.rmdb;

import com.centit.framework.core.dao.CodeBook;
import com.centit.framework.jdbc.dao.BaseDaoImpl;
import com.centit.product.metadata.dao.DataCheckRuleDao;
import com.centit.product.metadata.po.DataCheckRule;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;

/**
 * @author tian_y
 */
@Repository
public class DataCheckRuleDaoImpl extends BaseDaoImpl<DataCheckRule, String> implements DataCheckRuleDao {

    @Override
    public Map<String, String> getFilterField() {
        Map<String, String> filterField = new HashMap<>();
        filterField.put("ruleId" , CodeBook.EQUAL_HQL_ID);
        filterField.put("topUnit" , CodeBook.IN_HQL_ID);
        filterField.put("ruleType" , CodeBook.EQUAL_HQL_ID);
        filterField.put("ruleName" , CodeBook.EQUAL_HQL_ID);
        filterField.put("ruleFormula" , CodeBook.LIKE_HQL_ID);
        filterField.put("ruleParamSum" , CodeBook.LIKE_HQL_ID);
        filterField.put("ruleParamDesc" , CodeBook.LIKE_HQL_ID);
        filterField.put("faultMessage" , CodeBook.LIKE_HQL_ID);
        filterField.put("ruleDesc" , CodeBook.LIKE_HQL_ID);
        return filterField;
    }

}
