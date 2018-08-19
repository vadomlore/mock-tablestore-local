package com.siemens;


import com.alicloud.openservices.tablestore.model.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class MockSyncClient implements InvocationHandler {

    SimpleInMemoryTableStore store;

    public MockSyncClient(SimpleInMemoryTableStore store) {
        this.store = store;
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        switch (method.getName()) {
            case "getRow":
                return store.inMemoryTableInstanceManager.noConditionGetRow((GetRowRequest) args[0]);
            case "putRow":
                return store.inMemoryTableInstanceManager.noConditionPutRow((PutRowRequest) args[0]);
            case "updateRow":
                return store.inMemoryTableInstanceManager.noConditionUpdateRow((UpdateRowRequest) args[0]);
            case "createTable":
                return store.createTable((CreateTableRequest) args[0]);
            case "deleteTable":
                return store.deleteTable((DeleteTableRequest) args[0]);
            case "batchGetRow":
                return store.inMemoryTableInstanceManager.batchGetRow((BatchGetRowRequest) args[0]);
            case "batchWriteRow":
                return store.inMemoryTableInstanceManager.batchWriteRow((BatchWriteRowRequest) args[0]);
        }
        if (((method.getModifiers() & (Modifier.ABSTRACT | Modifier.PUBLIC | Modifier.STATIC)) ==
                Modifier.PUBLIC) && method.getDeclaringClass().isInterface()) {
            return method.invoke(args);
        }
        if (Object.class.equals(method.getDeclaringClass())) {
            try {
                return method.invoke(this, args);
            } catch (Throwable t) {
                throw t;
            }
        } else {
            throw new UnsupportedOperationException("operation not support yet");
        }
    }
}
