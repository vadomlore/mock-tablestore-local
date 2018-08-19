package com.siemens;

import com.alicloud.openservices.tablestore.model.*;

import java.util.HashMap;
import java.util.Map;


public class InMemoryTableInstance {

    Map<PrimaryKey, Row> dataInstance = new HashMap<>();


    Map<PrimaryKey, Row> getDataInstance() {
        return dataInstance;
    }

    void clear(){
        dataInstance.clear();
    }
}
