package com.centit.support.test;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 读取excel
 *
 * @author chen_l
 * @date 2019/3/7
 */
public class ReadExcel {

    //单元格的值出现公式时使用
    private static FormulaEvaluator evaluator;

    /**
     * 获取excel文件数据
     *
     * @param path excel文件的路径
     * @return dataMap excel文件的内容，格式{sheetName1:[[row1cell1,row1cell2,row1cell3],[row2cell1,row2cell2,row2cell3]],sheetName2:[[row1cell1,row1cell2,row1cell3],[row2cell1,row2cell2,row2cell3]]}
     */
    public Map<String, List<List<Object>>> getDataMap(String path) {
        Map<String, List<List<Object>>> dataMap = new HashMap<String, List<List<Object>>>();//存放excel数据
        try {
            File excelFile = new File(path);
            FileInputStream in = new FileInputStream(excelFile); // 文件流
            XSSFWorkbook wb = new XSSFWorkbook(in);
            int sheetNum = wb.getNumberOfSheets();//获取sheet页总数
            for (int sheetIndex = 0; sheetIndex < sheetNum; sheetIndex++) {
                Sheet sheet = wb.getSheetAt(sheetIndex);//获取sheet页
                String sheetName = sheet.getSheetName();//获取sheet名称，在map中作为键使用
                int rowNum = sheet.getLastRowNum();//获取sheet中数据的总行数，遍历行获取数据
                List<List<Object>> dataList = new ArrayList<List<Object>>();//存放sheet页数据，一行数据是一个List<String>
                for (Row row : sheet) {
                    List<Object> valueList = new ArrayList<Object>();//存放行数据
                    int cellNum = row.getLastCellNum();
                    for (int cellIndex = 0; cellIndex < cellNum; cellIndex++) {
                        Object cellValue = getCellValueByCell(row.getCell(cellIndex));
                        valueList.add(cellValue);//将单元格的值存放到行数据中
                    }
                    dataList.add(valueList);//将行数据存放到sheet数据中
                }
                dataMap.put(sheetName, dataList);//将sheet数据存放到excel数据中
            }
            in.close();//关闭文件流
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataMap;
    }

    /**
     * 获取单元格各类型值
     *
     * @param cell 单元格
     * @return 单元格的值
     */
    private Object getCellValueByCell(Cell cell) {
        //判断是否为null或空串
        if (cell == null || cell.toString().trim().equals("")) {
            return "";
        }
        Object cellText = new Object();
        String cellValue = "";
        int cellType = cell.getCellType();
        if (cellType == Cell.CELL_TYPE_FORMULA) { //表达式类型
            cellType = evaluator.evaluate(cell).getCellType();
        }

        switch (cellType) {
            case Cell.CELL_TYPE_STRING: //字符串类型
                cellValue = cell.getStringCellValue().trim();
//                cellValue = StringUtils.isEmpty(cellValue) ? "" : cellValue;
                cellText = cellValue;
                break;
            case Cell.CELL_TYPE_BOOLEAN:  //布尔类型
//                cellValue = String.valueOf(cell.getBooleanCellValue());
                cellText = cell.getBooleanCellValue();
                break;
            case Cell.CELL_TYPE_NUMERIC: //数值类型
                if (HSSFDateUtil.isCellDateFormatted(cell)) {  //判断日期类型
//                    cellValue = DateFormat.getDateTimeInstance().format(cell.getDateCellValue());
                    cellText = cell.getDateCellValue();
                } else {  //否
//                    cellValue = new DecimalFormat("#.#####").format(cell.getNumericCellValue());
                    cellText = cell.getNumericCellValue();
                }
                break;
            default: //其它类型，取空串
//                cellValue = "";
                cellText = "";
                break;
        }
//        System.out.println(cellType + ":" + cellValue);
        return cellText;
    }

    /**
     * 转换待处理或预期结果数据
     *
     * @param dataList  原格式的数据 举例：[[cell0-0，cell0-1，cell0-2...],[cell1-0，cell1-1，cell1-2...]...]
     * @param isSetNull 是否存入空值
     * @return dataMapList 新格式的数据 举例：[{cell0-0：cell1-0，cell0-1：cell1-1，cell0-2：cell1-2...},{cell0-0：cell2-0，cell0-1：cell2-1，cell0-2：cell2-2...}...]
     */
    public List<Map<String, Object>> getDataMapList(List<List<Object>> dataList, Boolean isSetNull) {
        List<Map<String, Object>> dataMapList = new ArrayList<Map<String, Object>>();
        List<Object> keyList = dataList.get(0);
        for (int dataIndex = 1; dataIndex < dataList.size(); dataIndex++) {
            Map<String, Object> dataMap = new HashMap<String, Object>();
            int dataSize = dataList.get(dataIndex).size();
            for (int keyIndex = 0; keyIndex < keyList.size(); keyIndex++) {
                String key = String.valueOf(keyList.get(keyIndex));
                if (keyIndex >= dataSize) {
                    dataList.get(dataIndex).add("");
                }
                Object value = dataList.get(dataIndex).get(keyIndex);
                if (value != "" || isSetNull == true) {
                    dataMap.put(key, value);
                }

            }
            dataMapList.add(dataMap);
        }
        return dataMapList;
    }

    /**
     * 转换排序、分组等字段集合
     *
     * @param fieldsList 原格式的数据 举例：[[cell0-0，cell0-1，cell0-2...],[cell1-0，cell1-1，cell1-2...]...]
     * @return fieldsMap 新格式的数据 举例：{cell0-0：[cell0-1，cell0-2...]，cell1-0：[cell1-1，cell1-2...]}
     */
    public Map<String, ArrayList<String>> getFieldsMap(List<List<Object>> fieldsList) {
        Map<String, ArrayList<String>> fieldsMap = new HashMap<String, ArrayList<String>>();
        for (int i = 0; i < fieldsList.size(); i++) {
            String key = String.valueOf(fieldsList.get(i).get(0));
            ArrayList<String> fields = new ArrayList<String>();
            for (int j = 1; j < fieldsList.get(i).size(); j++) {
                fields.add(String.valueOf(fieldsList.get(i).get(j)));
            }
            fieldsMap.put(key, fields);
        }
        return fieldsMap;
    }
}
