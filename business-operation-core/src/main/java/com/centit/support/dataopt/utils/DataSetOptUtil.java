package com.centit.support.dataopt.utils;

import com.centit.support.algorithm.BooleanBaseOpt;
import com.centit.support.algorithm.GeneralAlgorithm;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.compiler.VariableFormula;
import com.centit.support.dataopt.core.DataSet;
import com.centit.support.dataopt.core.SimpleDataSet;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.math3.stat.StatUtils;

import java.util.*;

public abstract class DataSetOptUtil {
    /**
     * 数据集 映射
     * @param inData 原始数据集
     * @param fieldMap 字段映射关系
     * @return 新的数据集
     */
    public static DataSet mapDateSet(DataSet inData, Collection<Map.Entry<String, String>> fieldMap) {
        List<Map<String, Object>> data = inData.getData();
        List<Map<String, Object>> newData = new ArrayList<>(data.size());

        for(Map<String, Object> obj : data ){
            Map<String, Object> newRow = new HashMap<>(fieldMap.size());
            for(Map.Entry<String, String> ent : fieldMap){
                newRow.put(ent.getKey(), obj.get(ent.getValue()));
            }
            newData.add(newRow);
        }
        return new SimpleDataSet(newData);
    }

    /**
     * 数据集 映射
     * @param inData 原始数据集
     * @param formulaMap 字段映射关系， value为计算表达式
     * @return 新的数据集
     */
    public static DataSet mapDateSetByFormula(DataSet inData,Collection<Map.Entry<String, String>> formulaMap) {
        List<Map<String, Object>> data = inData.getData();
        List<Map<String, Object>> newData = new ArrayList<>(data.size());
        for(Map<String, Object> obj : data ){
            Map<String, Object> newRow = new HashMap<>(formulaMap.size());
            for(Map.Entry<String, String> ent : formulaMap){
                newRow.put(ent.getKey(),
                    VariableFormula.calculate(ent.getValue(), obj));
            }
            newData.add(newRow);
        }
        return new SimpleDataSet(newData);
    }

    /**
     * 数据集 增加派生字段
     * @param inData 原始数据集
     * @param formulaMap 字段映射关系， value为计算表达式
     * @return 新的数据集 等同于原始数据集
     */
    public static DataSet appendDeriveField(DataSet inData,Collection<Map.Entry<String, String>> formulaMap) {
        List<Map<String, Object>> data = inData.getData();
        for(Map<String, Object> obj : data ){
            Map<String, Object> newRow = new HashMap<>(formulaMap.size());
            for(Map.Entry<String, String> ent : formulaMap){
                newRow.put(ent.getKey(),
                    VariableFormula.calculate(ent.getValue(), obj));
            }
            obj.putAll(newRow);
        }
        return inData;
    }
    /**
     * 数据集 映射
     * @param inData 原始数据集
     * @param formula 逻辑表达式
     * @return 新的数据集
     */
    public static DataSet filterDateSet(DataSet inData,String formula) {
        List<Map<String, Object>> data = inData.getData();
        List<Map<String, Object>> newData = new ArrayList<>(data.size());
        for(Map<String, Object> obj : data ){
           if(BooleanBaseOpt.castObjectToBoolean(
                    VariableFormula.calculate(formula, obj),false)){
               newData.add(obj);
            }
        }
        SimpleDataSet outDataSet = new SimpleDataSet(newData);
        outDataSet.setDataSetType(inData.getDataSetName());
        return outDataSet;
    }

    /**
     * 对数据集进行排序
     * @param inData 原始数据集
     * @param fields 排序字段
     * @return 排序后的数据集，修改了原始数据集的数据顺序
     */
    public static DataSet sortDataSetByFields(DataSet inData, List<String> fields) {
        sortByFields(inData.getData(),fields);
        return inData;
    }

    private static double[] listDoubleToArray(List<Double> dblist){
        double[] dbs = new double[dblist.size()];
        int i=0;
        for(Double db : dblist){
            dbs[i++] = db!=null? db : 0.0;
        }
        return dbs;
    }

    private static Map<String, Object> makeNewStatRow(List<String> groupbyFields,
                                                      List<Triple<String, String,String>> statDesc,
                                                      Map<String, Object> preRow,
                                                      Map<String, List<Double>> tempData){
        Map<String, Object> newRow = new HashMap<>();
        if(groupbyFields != null && groupbyFields.size()>0) {
            for (String field : groupbyFields) {
                newRow.put(field, preRow.get(field));
            }
        }
        for(Triple<String, String,String> tr :  statDesc){
            Double db = null;
            switch (tr.getRight()){
                case "min":
                    db = StatUtils.min(listDoubleToArray(tempData.get(tr.getLeft())));
                    break;
                case "max":
                    db = StatUtils.max(listDoubleToArray(tempData.get(tr.getLeft())));
                    break;
                case "mean":
                    db = StatUtils.mean(listDoubleToArray(tempData.get(tr.getLeft())));
                    break;
                case "sum":
                    db = StatUtils.sum(listDoubleToArray(tempData.get(tr.getLeft())));
                    break;
                case "sumSq":
                    db = StatUtils.sumSq(listDoubleToArray(tempData.get(tr.getLeft())));
                    break;
                case "prod":
                    db = StatUtils.product(listDoubleToArray(tempData.get(tr.getLeft())));
                    break;
                case "sumLog":
                    db = StatUtils.sumLog(listDoubleToArray(tempData.get(tr.getLeft())));
                    break;
                case "geometricMean":
                    db = StatUtils.geometricMean(listDoubleToArray(tempData.get(tr.getLeft())));
                    break;
                case "variance":
                    db = StatUtils.variance(listDoubleToArray(tempData.get(tr.getLeft())));
                    break;
                /* percentile 这个没有实现*/
            }
            newRow.put(tr.getLeft(), db);
        }
        return newRow;
    }

    public static DataSet statDataset2(DataSet inData,
                                      List<String> groupbyFields,
                                      Collection<Map.Entry<String, String>> statDesc) {
        List<Triple<String, String,String>> sd = new ArrayList<>(statDesc.size());
        for (Map.Entry<String, String> s: statDesc){
            sd.add(new MutableTriple<>(s.getKey()+':'+s.getValue(),s.getKey(),s.getValue()));
        }
        return statDataset(inData,groupbyFields,sd);
    }
    /**
     * 分组统计 , 如果 List&gt;String&lt; groupbyFields 为null 或者 空 就是统计所有的（仅返回一行）
     * @param inData 输入数据集
     * @param groupbyFields 分组（排序）字段
     * @param statDesc  统计描述; 新字段名， 源字段名， 统计方式 （求和，最大，最小，平均，方差，标准差）
     * @return 返回数据集
     */
    public static DataSet statDataset(DataSet inData,
                                      List<String> groupbyFields,
                                      List<Triple<String, String, String>> statDesc) {
        if (inData == null) {
            return null;
        }
        List<Map<String, Object>> data = inData.getData();
        if (data == null || data.size() == 0) {
            return inData;
        }
        //按group by字段排序
        if(groupbyFields != null && groupbyFields.size()>0) {
            sortByFields(data, groupbyFields);
        }

        List<Map<String, Object>> newData = new ArrayList<>();
        Map<String, Object> preRow = null;

        Map<String, List<Double>> tempData = new HashMap<>();
        for(Triple<String, String,String> tr :  statDesc){
            tempData.put(tr.getLeft(), new ArrayList<>());
        }

        for(Map<String, Object> row : data){
            if(compareTwoRow(preRow,row,groupbyFields) !=0 ){
                if(preRow!=null){
                    //保存newRow
                    Map<String, Object> newRow =makeNewStatRow(groupbyFields,
                         statDesc,preRow, tempData);
                    newData.add(newRow);
                }
                // 新建数据临时数据空间
                for(Triple<String, String,String> tr :  statDesc){
                    tempData.get(tr.getLeft()).clear();
                }
            }
            for(Triple<String, String,String> tr :  statDesc){
                tempData.get(tr.getLeft()).add(
                    NumberBaseOpt.castObjectToDouble(row.get(tr.getMiddle())));
            }
            preRow = row;
        }

        if(preRow!=null){
            //保存newRow
            Map<String, Object> newRow =makeNewStatRow(groupbyFields,
                statDesc,preRow, tempData);
            newData.add(newRow);
        }
        return new SimpleDataSet(newData);
    }

    public static void analyseDatasetGroup( List<Map<String, Object>> data,
                                            int offset, int endPos,
                                            DatasetVariableTranslate dvt,
                                            Collection<Map.Entry<String, String>> refDesc) {
        dvt.setOffset(offset);
        dvt.setLength(endPos-offset);
        for(int j = offset; j<endPos; j++) {
            Map<String, Object> newRow = data.get(j);
            dvt.setCurrentPos(j);
            for (Map.Entry<String, String> ref : refDesc) {
                newRow.put(ref.getKey(),
                    VariableFormula.calculate(ref.getValue(), dvt));
            }
        }
    }
    /**
     * 分组统计 , 如果 List&gt;String&lt; groupbyFields 为null 或者 空 就是统计所有的（仅返回一行）
     * @param inData 输入数据集
     * @param groupbyFields 分组字段
     * @param orderbyFields 排序字段
     * @param refDesc  引用说明; 新字段名， 引用表达式
     * @return 返回数据集
     */
    public static DataSet analyseDataset(DataSet inData,
                                        List<String> groupbyFields,
                                        List<String> orderbyFields,
                                        Collection<Map.Entry<String, String>> refDesc) {
        List<Map<String, Object>> data = inData.getData();
        List<String> keyRows = ListUtils.union(groupbyFields, orderbyFields);
        //根据维度进行排序 行头、列头
        sortByFields(data, keyRows);
        Map<String, Object> preRow = null;
        int n = data.size();
        int prePos = 0;
        DatasetVariableTranslate dvt = new DatasetVariableTranslate(data);
        //int endPos = 0;
        for(int i=0; i<n; i++){
            Map<String, Object> row = data.get(i);
            if(compareTwoRow(preRow,row, groupbyFields) !=0 ){
                if(preRow != null){
                    analyseDatasetGroup(data,prePos,i,dvt,refDesc);
                }
                prePos = i;
            }
            preRow = row;
        }
        analyseDatasetGroup(data,prePos,n,dvt,refDesc);
        return inData;
    }
    /***
     * 交叉制表 数据处理
     * @param inData 输入数据集
     * @param colHeaderFields 列头信息
     * @param rowHeaderFields 行头信息
     * @return 输出数据集
     */
    public static DataSet crossTabulation(DataSet inData, List<String> rowHeaderFields, List<String> colHeaderFields) {
        if (inData == null) {
            return null;
        }
        List<Map<String, Object>> data = inData.getData();
        if (data == null || data.size() == 0) {
            return inData;
        }
        if (rowHeaderFields.size() + colHeaderFields.size() >= data.get(0).size()) {
            throw new RuntimeException("数据不合法");
        }
        List<String> keyRows = ListUtils.union(rowHeaderFields, colHeaderFields);
        //根据维度进行排序 行头、列头
        sortByFields(data, keyRows);
        List<Map<String, Object>> newData = new ArrayList<>();
        Map<String, Object> preRow = null;
        Map<String, Object> newRow = null;
        for(Map<String, Object> row : data){
            if(compareTwoRow(preRow,row, rowHeaderFields) !=0 ){
                if(preRow!=null && newRow!=null){
                    newData.add(newRow);
                }
                // 新建数据临时数据空间
                newRow = new HashMap<>();
                for(String key :rowHeaderFields){
                    newRow.put(key, row.get(key));
                }
            }

            StringBuilder colprefix = new StringBuilder();
            for(String key : colHeaderFields){
                colprefix.append(key).append(":").append(row.get(key)).append(":");
            }

            String prefix = colprefix.toString();
            for(Map.Entry<String, Object> entry : row.entrySet()){
                String key = entry.getKey();
                if(!keyRows.contains(key)){
                    newRow.put(prefix + key, entry.getValue());
                }
            }
            preRow = row;
        }

        if(preRow!=null && newRow!=null){
            newData.add(newRow);
        }
        return new SimpleDataSet(newData);
    }

    private static void appendData(Map<String, Object> newRow, Map<String, Object> oldData,
                                   List<String> primaryFields, String suffix, boolean appendKey ){

        for(Map.Entry<String, Object> entry : oldData.entrySet()){
            String key = entry.getKey();
            if(primaryFields.contains(key)){
                if(appendKey) {
                    newRow.put(key, entry.getValue());
                }
            }else{
                newRow.put(key + suffix, entry.getValue());
            }
        }
    }
    /**
     * 同环比转换
     * @param currDataSet 本期数据集
     * @param lastDataSet 上期数据集
     * @param primaryFields 主键列
     * @return DataSet
     */
    public static DataSet compareTabulation(DataSet currDataSet, DataSet lastDataSet, List<String> primaryFields) {
        if (currDataSet == null || lastDataSet == null) {
            return null;
        }
        List<Map<String, Object>> currData = currDataSet.getData();
        List<Map<String, Object>> lastData = lastDataSet.getData();
        if (currData == null || lastData == null ) {
            throw new RuntimeException("数据不合法");
        }

        List<Map<String, Object>> newData = new ArrayList<>();
        // 根据主键排序
        sortByFields(currData, primaryFields);
        sortByFields(lastData, primaryFields);
        int i=0;
        int j=0;
        while(i < currData.size() && j< lastData.size()){
            int nc = compareTwoRow(currData.get(i), lastData.get(j), primaryFields);
            //匹配
            Map<String, Object> newRow = new LinkedHashMap<>();
            if(nc == 0){
                appendData(newRow, currData.get(i), primaryFields,":current",true);
                appendData(newRow, lastData.get(j), primaryFields,":last",false);
                i++; j++;
            } else if(nc < 0){
                appendData(newRow, currData.get(i), primaryFields,":current",true);
                i++;
            } else {
                appendData(newRow, lastData.get(j), primaryFields,":last",true);
                j++;
            }
            newData.add(newRow);
        }

        while(i < currData.size()){
            Map<String, Object> newRow = new LinkedHashMap<>();
            appendData(newRow, currData.get(i), primaryFields,":current",true);
            newData.add(newRow);
            i++;
        }

        while(j< lastData.size()){
            Map<String, Object> newRow = new LinkedHashMap<>();
            appendData(newRow, lastData.get(j), primaryFields,":last",true);
            newData.add(newRow);
            j++;
        }
        return new SimpleDataSet(newData);
    }

    /**
     * 合并两个数据集
     * @param mainDataSet  主数据集
     * @param slaveDataSet 次数据集
     * @param primaryFields 主键列
     * @return DataSet
     */
    public static DataSet joinTwoDataSet(DataSet mainDataSet, DataSet slaveDataSet, List<String> primaryFields) {
        if (mainDataSet == null) {
            return slaveDataSet;
        }
        if(slaveDataSet == null){
            return mainDataSet;
        }

        List<Map<String, Object>> mainData = mainDataSet.getData();
        List<Map<String, Object>> slaveData = slaveDataSet.getData();


        sortByFields(mainData, primaryFields);
        sortByFields(slaveData, primaryFields);
        int i=0;
        int j=0;
        List<Map<String, Object>> newData = new ArrayList<>();
        // 根据主键排序
        while(i < mainData.size() && j< slaveData.size()){
            int nc = compareTwoRow(mainData.get(i), slaveData.get(j), primaryFields);
            //匹配
            Map<String, Object> newRow = new LinkedHashMap<>();
            if(nc == 0){
                newRow.putAll(slaveData.get(j));
                newRow.putAll(mainData.get(i));
                i++; j++;
            } else if(nc < 0){
                newRow.putAll(mainData.get(i));
                i++;
            } else {
                newRow.putAll(slaveData.get(j));
                j++;
            }
            newData.add(newRow);
        }

        while(i < mainData.size()){
            Map<String, Object> newRow = new LinkedHashMap<>();
            newRow.putAll(mainData.get(i));
            newData.add(newRow);
            i++;
        }

        while(j< slaveData.size()){
            Map<String, Object> newRow = new LinkedHashMap<>();
            newRow.putAll(slaveData.get(j));
            newData.add(newRow);
            j++;
        }
        return new SimpleDataSet(newData);
    }

    private static int compareTwoRow(Map<String, Object> data1, Map<String, Object> data2, List<String> fields) {
        if(data1 == null && data2 == null){
            return 0;
        }
        if(data1 == null){
            return -1;
        }
        if(data2 == null){
            return 1;
        }
        if(fields == null){
            return 0;
        }
        for (String field : fields) {
            if(field.endsWith(" desc")){
                String dataField = field.substring(0,field.length()-5).trim();
                int cr = GeneralAlgorithm.compareTwoObject(
                    data1.get(dataField), data2.get(dataField));
                if (cr != 0) {
                    return 0 - cr;
                }
            } else {
                int cr = GeneralAlgorithm.compareTwoObject(
                    data1.get(field), data2.get(field));
                if (cr != 0) {
                    return cr;
                }
            }
        }
        return 0;
    }

    private static void sortByFields(List<Map<String, Object>> data, List<String> fields) {
        data.sort( (o1, o2) -> compareTwoRow(o1, o2, fields));
    }
}
