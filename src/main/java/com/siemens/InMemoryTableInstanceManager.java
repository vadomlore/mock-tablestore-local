package com.siemens;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class InMemoryTableInstanceManager {

    Map<String, InMemoryTableInstance> inMemoryTableInstanceMap = new HashMap<>();

    public void addInstance(String tableName){
        inMemoryTableInstanceMap.put(tableName, new InMemoryTableInstance());
    }

    public InMemoryTableInstance getInstance(String tableName){
        return inMemoryTableInstanceMap.get(tableName);
    }

    public InMemoryTableInstance clear(String tableName){
        InMemoryTableInstance inMemoryTableInstance = inMemoryTableInstanceMap.get(tableName);
        if(inMemoryTableInstance == null) return null;
        inMemoryTableInstance.clear();
        return inMemoryTableInstance;
    }

    public void clear(){
        for (Map.Entry<String, InMemoryTableInstance> kvs : inMemoryTableInstanceMap.entrySet()) {
            kvs.getValue().clear();
        }
        this.inMemoryTableInstanceMap.clear();
    }


    private Lock lock = new ReentrantLock();
    /**
     * 无条件更新, 如果该列不存在，则创建该列
     * RowUpdateChange put row into InMemoryTableStore
     *
     * @param updateRowRequest
     * @return
     */
    public UpdateRowResponse noConditionUpdateRow(UpdateRowRequest updateRowRequest) {
        RowUpdateChange rowUpdate = (RowUpdateChange) updateRowRequest.getRowChange();
        PrimaryKey primaryKey = rowUpdate.getPrimaryKey();
        String tableName = rowUpdate.getTableName();
        try {
            lock.lock();
            InMemoryTableInstance memoryStore = inMemoryTableInstanceMap.get(tableName);
            Map<PrimaryKey, Row> dataInstance = memoryStore.getDataInstance();
            if (!dataInstance.containsKey(primaryKey)) {
                return new UpdateRowResponse(new Response(), null, new ConsumedCapacity(
                        new CapacityUnit()
                ));
            }

            for (Pair<Column, RowUpdateChange.Type> columnTypePair : rowUpdate.getColumnsToUpdate()) {
                RowUpdateChange.Type type = columnTypePair.getSecond();
                Column columnTarget = columnTypePair.getFirst();
                switch (type) {
                    case DELETE_ALL:
                        Row row = dataInstance.get(primaryKey);
                        List<Column> columnsToExclude = row.getColumn(columnTarget.getName());
                        List<Column> remainColumns = getFilterColumns(row.getColumns(), columnsToExclude.toArray(new Column[0]));
                        dataInstance.put(primaryKey, new Row(primaryKey, remainColumns));
                        break;
                    case DELETE:
                        row = dataInstance.get(primaryKey);
                        remainColumns = Arrays.stream(row.getColumns()).filter(
                                x -> !x.getName().equals(columnTarget.getName()) &&
                                        !(x.getTimestamp() == columnTarget.getTimestamp())).collect(Collectors.toList());
                        dataInstance.put(primaryKey, new Row(primaryKey, remainColumns));
                        break;
                    case PUT:
                        row = dataInstance.get(primaryKey);
                        List<Column> columns = row.getColumn(columnTarget.getName());
                        if(!columnTarget.hasSetTimestamp()){ //没有设置时间戳表示更新最新时间戳的数据
                            Column latestColumn = row.getLatestColumn(columnTarget.getName());
                            if(row.getLatestColumn(columnTarget.getName()) != null){
                                List<Column> remains = getFilterColumns(row.getColumns(), new Column[]{latestColumn});
                                remains.add(new Column(columnTarget.getName(), columnTarget.getValue(),
                                        columnTarget.hasSetTimestamp()?columnTarget.getTimestamp(): System.nanoTime()));
                                dataInstance.put(primaryKey, new Row(primaryKey, remains));
                            }
                        }
                        else {

                            columns.stream().filter(x -> x.getTimestamp() == columnTarget.getTimestamp() && x.getName().equals(columnTarget.getName())).findFirst().ifPresent(
                                    x -> {
                                        List<Column> remains = getFilterColumns(row.getColumns(), new Column[]{x});
                                        remains.add(new Column(columnTarget.getName(), columnTarget.getValue(),
                                                columnTarget.hasSetTimestamp() ? columnTarget.getTimestamp() : System.nanoTime()));
                                        dataInstance.put(primaryKey, new Row(primaryKey, remains));
                                    }
                            );
                        }
                        break;
                }
            }
            return new UpdateRowResponse(new Response(), dataInstance.get(primaryKey), new ConsumedCapacity(
                    new CapacityUnit()));

        } finally {
            lock.unlock();
        }
    }


    /**
     * 简易版无条件插入
     * RowPutChange put row into InMemoryTableStore
     *
     * @param putRowRequest
     * @return
     */
    public PutRowResponse noConditionPutRow(PutRowRequest putRowRequest) {
        RowPutChange rowChange = (RowPutChange) putRowRequest.getRowChange();
        PrimaryKey primaryKey = rowChange.getPrimaryKey();
        String tableName = rowChange.getTableName();
        try {
            lock.lock();
            InMemoryTableInstance memoryStore = inMemoryTableInstanceMap.get(tableName);
            Map<PrimaryKey, Row> dataInstance = memoryStore.getDataInstance();
            if (!dataInstance.containsKey(primaryKey)) {
                List<Column> columnWithTimestamp = withTimestamp(rowChange, System.nanoTime());
                Row row = new Row(primaryKey, columnWithTimestamp);

                dataInstance.put(primaryKey, row);
                return new PutRowResponse(new Response(), dataInstance.get(primaryKey), new ConsumedCapacity(
                        new CapacityUnit()
                ));
            } else {
                List<Column> columnWithTimestamp = withTimestamp(rowChange, System.nanoTime());
                Row oldRow = dataInstance.get(primaryKey);

                for (Column column : columnWithTimestamp) {
                    //copy on write
                    Column[] columns = oldRow.getColumns();
                    int len = columns.length;
                    Column[] newElements = Arrays.copyOf(columns, len + 1);
                    newElements[len] = column;
                    dataInstance.put(primaryKey, new Row(primaryKey, newElements));
                }
                return new PutRowResponse(new Response(), dataInstance.get(primaryKey), new ConsumedCapacity(
                        new CapacityUnit()
                ));
            }
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * 简易版获取一条Row的记录(读取出完整的Column以及版本记录)
     * RowPutChange put row into InMemoryTableStore
     *
     * @param getRowRequest
     * @return
     */
    public GetRowResponse noConditionGetRow(GetRowRequest getRowRequest) {
        SingleRowQueryCriteria singleRowQueryCriteria = getRowRequest.getRowQueryCriteria();
        PrimaryKey primaryKey = singleRowQueryCriteria.getPrimaryKey();
        try {
            lock.lock();
            InMemoryTableInstance memoryStore = inMemoryTableInstanceMap.get(singleRowQueryCriteria.getTableName());
            Map<PrimaryKey, Row> dataInstance = memoryStore.getDataInstance();
            return new GetRowResponse(new Response(), dataInstance.get(primaryKey), new ConsumedCapacity(
                    new CapacityUnit()
            ));
        } finally {
            lock.unlock();
        }
    }

    /**
     * batch write rows into memory
     * @param batchWriteRowRequest
     * @return
     * @throws TableStoreException
     * @throws ClientException
     */
    public BatchWriteRowResponse batchWriteRow(final BatchWriteRowRequest batchWriteRowRequest)
            throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(batchWriteRowRequest);
        Map<String, List<RowChange>> batchWriteRow = batchWriteRowRequest.getRowChange();
        BatchWriteRowResponse response = new BatchWriteRowResponse(new Response());

        for (Map.Entry<String, List<RowChange>> kvs: batchWriteRow.entrySet()) {
            String tableName = kvs.getKey();
            List<RowChange> rowChanges = kvs.getValue();
            for (RowChange rowChange : rowChanges) {
                int index = 0;
                if(rowChange instanceof RowUpdateChange){
                    UpdateRowResponse response0 = noConditionUpdateRow(new UpdateRowRequest((RowUpdateChange) rowChange));
                    response.addRowResult(new BatchWriteRowResponse.RowResult(tableName, response0.getRow(),  new ConsumedCapacity(new CapacityUnit()), index++));
                }
                else if(rowChange instanceof RowPutChange){
                    PutRowResponse response0 = noConditionPutRow(new PutRowRequest((RowPutChange) rowChange));
                    response.addRowResult(new BatchWriteRowResponse.RowResult(tableName, response0.getRow(),  new ConsumedCapacity(new CapacityUnit()), index++));
                }
                //todo 添加DeleteRow 操作
                else{
                    throw new UnsupportedOperationException("operation unimplement yet.");
                }
            }
        }
        return response;
    }

    public BatchGetRowResponse batchGetRow(final BatchGetRowRequest batchGetRowRequest)
            throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(batchGetRowRequest);
        BatchGetRowResponse response = new BatchGetRowResponse(new Response());
        for (Map.Entry<String, MultiRowQueryCriteria> kvs:  batchGetRowRequest.getCriteriasByTable().entrySet()) {
            String tableName = kvs.getKey();
            MultiRowQueryCriteria criterias = kvs.getValue();
            for (int i = 0; i < criterias.getRowKeys().size(); i++) {
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(new SingleRowQueryCriteria(tableName, criterias.get(i)));
                GetRowResponse response0 = noConditionGetRow(request);
                response.addResult(new BatchGetRowResponse.RowResult(tableName, response0.getRow(),  new ConsumedCapacity(new CapacityUnit()), i));
            }
        }
        return response;
    }



    /**
     * get removed collection from exists collection
     *
     * @param columns          original coluns
     * @param elementsToRemove elements to exclude
     * @return remain elements
     */
    public List<Column> getFilterColumns(Column[] columns, Column[] elementsToRemove) {
        return Arrays.stream(columns).filter(x ->
                Arrays.stream(elementsToRemove).noneMatch(y -> y.equals(x)
                )).collect(Collectors.toList());
    }

    /**
     * @param columns          original columns
     * @param elementsToRemove elements to exclude
     * @return remain elements
     */
    public List<Column> getFilterColumns(List<Column> columns, List<Column> elementsToRemove) {
        return columns.stream().filter(x ->
                elementsToRemove.stream().noneMatch(y -> y.equals(x)
                )).collect(Collectors.toList());
    }

    /**
     * write timestamp to RowPutChange column if the column timestamp is not set.
     *
     * @param rowChange
     * @param timestamp
     * @return
     */
    private List<Column> withTimestamp(RowPutChange rowChange, long timestamp) {
        List<Column> columnList = rowChange.getColumnsToPut();
        return columnList.stream().map(x -> {
            if (!x.hasSetTimestamp()) {
                return new Column(x.getName(), x.getValue(), timestamp);
            } else {
                return x;
            }
        }).collect(Collectors.toList());
    }
}
