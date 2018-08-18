package com.siemens;

import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.model.*;
import com.google.common.collect.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


public class InMemoryTableInstance {

    //reference of table
    TableMeta meta;

    Map<PrimaryKey, Row> dataInstance =
            Maps.newLinkedHashMap();

    public InMemoryTableInstance(TableMeta tableMeta) {
        meta = tableMeta;
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
        try {
            lock.lock();
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
        if (!dataInstance.containsKey(primaryKey)) {
            List<Column> columnWithTimestamp = withTimestamp(rowChange, System.nanoTime());
            Row row = new Row(primaryKey, columnWithTimestamp);
            try {
                lock.lock();

                dataInstance.put(primaryKey, row);
                return new PutRowResponse(new Response(), dataInstance.get(primaryKey), new ConsumedCapacity(
                        new CapacityUnit()
                ));
            } finally {
                lock.unlock();
            }
        } else {
            List<Column> columnWithTimestamp = withTimestamp(rowChange, System.nanoTime());
            Row oldRow = dataInstance.get(primaryKey);
            try {
                lock.lock();
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
            } finally {
                lock.unlock();
            }
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
            return new GetRowResponse(new Response(), dataInstance.get(primaryKey), new ConsumedCapacity(
                    new CapacityUnit()
            ));
        } finally {
            lock.unlock();
        }
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
}
