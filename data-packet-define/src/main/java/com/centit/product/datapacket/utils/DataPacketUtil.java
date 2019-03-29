package com.centit.product.datapacket.utils;

import com.alibaba.fastjson.JSONObject;
import com.centit.product.datapacket.vo.DataPacketSchema;

public abstract class DataPacketUtil {
    public static DataPacketSchema calcDataPacketSchema(DataPacketSchema sourceSchema, JSONObject obj){

        return sourceSchema;
    }
}
