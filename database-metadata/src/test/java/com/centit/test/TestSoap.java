package com.centit.test;

import com.alibaba.fastjson2.JSON;
import com.centit.support.network.HttpExecutor;
import com.centit.support.network.HttpExecutorContext;
import org.dom4j.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSoap {

    public static Element findOperationElement(Element rootElement, String typeName) {
        List<Element> portTypes = rootElement.elements( "portType");
        for (Element portType : portTypes) {
            for (Element operation : portType.elements( "operation")) {
                Attribute nameAttr = operation.attribute("name");
                if (nameAttr != null && typeName.equals(nameAttr.getValue())){
                    return operation;
                }
            }
        }
        return null;
    }

    public static Element findTypeElement(Element portTypes, String typeName) {
        Element typeElement = portTypes.element( "schema");
        for( Element schema : typeElement.elements("element")){
            if( schema.attribute("name").getValue().equals(typeName)){
                return schema;
            }
        }
        return null;
    }

    public static Map<String, String> mapElementType(Element operationElem) {
        Map<String, String> stringMap = new HashMap<>();
        Element element = operationElem.element("complexType");
        if( element == null) return stringMap;
        element = element.element("sequence");
        if( element == null) return stringMap;
        for( Element elem : element.elements("element")){
                String name = elem.attribute("name").getValue();
                String type = elem.attribute("type").getValue();
                stringMap.put(name, type);
        }
        return stringMap;
    }
    public static void main(String arg[]) {

        try {
            String wsdl = HttpExecutor.simpleGet(HttpExecutorContext.create(),
                "http://192.168.132.70/WebService/DataExchange.asmx?wsdl");
            Document doc = DocumentHelper.parseText(wsdl);
            Element portTypes = doc.getRootElement().element( "types");
            Element operationElem = findTypeElement(portTypes,"GetData");
            if(operationElem==null){
                return;
            }
            Map<String, String> stringMap = mapElementType(operationElem);
            System.out.println(JSON.toJSONString(stringMap));
        } catch (IOException | DocumentException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Done!");
    }
}
