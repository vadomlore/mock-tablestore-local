package com.siemens;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Proxy;

public class TestMockSyncInterface {

    SimpleInMemoryTableStore simpleInMemoryTableStore = new SimpleInMemoryTableStore();

    SyncClientInterface syncClient = (SyncClientInterface) Proxy.newProxyInstance(ClassLoader.getSystemClassLoader(), SyncClient.class.getInterfaces(), new MockSyncClient(
            simpleInMemoryTableStore
    ));


    @Test
    public void doDefault() {
        Assert.assertNotNull(syncClient.toString());
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testNotSupportClass() {
        syncClient.asAsyncClient();
    }


    @Before
    public void setUp() {
        simpleInMemoryTableStore.clean();
    }

    @After
    public void tearDown() {
        simpleInMemoryTableStore.clean();
    }


    private static final String CST = "cts_table";


    @Test
    public void testCreateTable() {
        TableMeta meta = createTable();
        Assert.assertEquals(simpleInMemoryTableStore.getTableMeta(CST), meta);
    }

    private TableMeta createTable() {
        TableMeta meta = new TableMeta(CST);
        meta.addPrimaryKeyColumn(
                new PrimaryKeySchema("pk1", PrimaryKeyType.STRING));
        CreateTableRequest createTableRequest = new CreateTableRequest(meta
                , new TableOptions());
        syncClient.createTable(createTableRequest);
        return meta;
    }

    @Test(expected = TableStoreException.class)
    public void testCreateTableWhenTableExists() {
        TableMeta meta = new TableMeta(CST);
        meta.addPrimaryKeyColumn(
                new PrimaryKeySchema("pk1", PrimaryKeyType.STRING));
        CreateTableRequest createTableRequest = new CreateTableRequest(meta
                , new TableOptions());
        syncClient.createTable(createTableRequest);
        syncClient.createTable(createTableRequest);
    }

    public void deleteTable() {
        TableMeta meta = createTable();

        syncClient.deleteTable(new DeleteTableRequest(CST));
        Assert.assertNull(simpleInMemoryTableStore.getTableMeta(CST));
    }


    @Test(expected = TableStoreException.class)
    public void deleteTableWhenTableNotExists() {
        TableMeta meta = createTable();

        syncClient.deleteTable(new DeleteTableRequest(CST));
        syncClient.deleteTable(new DeleteTableRequest(CST));
    }

    @Test
    public void testPutRow() {
        createTable();

        PutRowRequest putRowRequest = new PutRowRequest();
        PrimaryKey primaryKey = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("aa")))
                .build();
        RowPutChange change = new RowPutChange(CST,
                primaryKey);
        change.addColumn(new Column("co1", ColumnValue.fromBoolean(true)));
        change.addColumn(new Column("co2", ColumnValue.fromString("abbb")));
        change.addColumn(new Column("co3", ColumnValue.fromLong(13445)));
        putRowRequest.setRowChange(change);
        syncClient.putRow(putRowRequest);

        Assert.assertNotNull(simpleInMemoryTableStore.getInMemoryTableInstance(CST).dataInstance.get(primaryKey));
        Assert.assertEquals(simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance
                .get(primaryKey)
                .getLatestColumn("co1").getValue().asBoolean(), true);

        Assert.assertEquals(simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance
                .get(primaryKey)
                .getLatestColumn("co2").getValue().asString(), "abbb");

        Assert.assertEquals(simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance
                .get(primaryKey)
                .getLatestColumn("co3").getValue().asLong(), 13445);

    }

    @Test
    public void testBatchPutRowAndResponse() {
        createTable();

        BatchWriteRowRequest putRowRequest = new BatchWriteRowRequest();

        PrimaryKey primaryKey0 = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("aa")))
                .build();
        RowPutChange change0 = new RowPutChange(CST,
                primaryKey0);
        change0.addColumn(new Column("co0", ColumnValue.fromBoolean(true)));
        change0.addColumn(new Column("co1", ColumnValue.fromString("a")));
        change0.addColumn(new Column("co2", ColumnValue.fromLong(1)));
        putRowRequest.addRowChange(change0);

        PrimaryKey primaryKey1 = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("bb")))
                .build();
        RowPutChange change1 = new RowPutChange(CST,
                primaryKey1);
        change1.addColumn(new Column("co0", ColumnValue.fromBoolean(false)));
        change1.addColumn(new Column("co1", ColumnValue.fromString("b")));
        change1.addColumn(new Column("co2", ColumnValue.fromLong(2)));
        putRowRequest.addRowChange(change1);
        syncClient.batchWriteRow(putRowRequest);



        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        MultiRowQueryCriteria queryCriteria = new MultiRowQueryCriteria(CST);
        queryCriteria.addRow(primaryKey0);
        queryCriteria.addRow(primaryKey1);

        batchGetRowRequest.addMultiRowQueryCriteria(queryCriteria);
        BatchGetRowResponse response = syncClient.batchGetRow(batchGetRowRequest);
        Assert.assertEquals(2, response.getBatchGetRowResult(CST).size());
        Assert.assertEquals(response.getBatchGetRowResult(CST).get(0).getRow().getLatestColumn("co0").getValue().asBoolean(), true);
        Assert.assertEquals(response.getBatchGetRowResult(CST).get(0).getRow().getLatestColumn("co1").getValue().asString(), "a");
        Assert.assertEquals(response.getBatchGetRowResult(CST).get(0).getRow().getLatestColumn("co2").getValue().asLong(), 1);
        Assert.assertEquals(response.getBatchGetRowResult(CST).get(1).getRow().getLatestColumn("co0").getValue().asBoolean(), false);
        Assert.assertEquals(response.getBatchGetRowResult(CST).get(1).getRow().getLatestColumn("co1").getValue().asString(), "b");
        Assert.assertEquals(response.getBatchGetRowResult(CST).get(1).getRow().getLatestColumn("co2").getValue().asLong(), 2);
    }

    @Test
    public void testGetRow() {
        createTable();

        PutRowRequest putRowRequest = new PutRowRequest();
        PrimaryKey primaryKey = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("aa")))
                .build();
        RowPutChange change = new RowPutChange(CST,
                primaryKey);
        change.addColumn(new Column("co1", ColumnValue.fromBoolean(true)));
        change.addColumn(new Column("co2", ColumnValue.fromString("abbb")));
        change.addColumn(new Column("co3", ColumnValue.fromLong(13445)));
        putRowRequest.setRowChange(change);
        syncClient.putRow(putRowRequest);

        GetRowRequest getRowRequest = new GetRowRequest();
        PrimaryKey primaryKey1 = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("aa")))
                .build();
        SingleRowQueryCriteria singleRowQueryCriteria = new SingleRowQueryCriteria(CST);
        singleRowQueryCriteria.setPrimaryKey(primaryKey1);
        getRowRequest.setRowQueryCriteria(singleRowQueryCriteria);
        GetRowResponse response = syncClient.getRow(getRowRequest);

        Assert.assertEquals(response.getRow().getColumns().length, 3);
        Assert.assertEquals(response.getRow().getLatestColumn("co1").getValue().asBoolean(), true);
        Assert.assertEquals(response.getRow().getLatestColumn("co2").getValue().asString(), "abbb");
        Assert.assertEquals(response.getRow().getLatestColumn("co3").getValue().asLong(), 13445);
    }


    @Test
    public void testGetRowWithVersion() {
        createTable();
        PutRowRequest putRowRequest = new PutRowRequest();
        PrimaryKey primaryKey = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("aa")))
                .build();
        RowPutChange change = new RowPutChange(CST,
                primaryKey);
        change.addColumn(new Column("co1", ColumnValue.fromBoolean(true)));
        change.addColumn(new Column("co2", ColumnValue.fromString("abbb")));
        change.addColumn(new Column("co3", ColumnValue.fromLong(13445)));
        putRowRequest.setRowChange(change);
        syncClient.putRow(putRowRequest);

        GetRowRequest getRowRequest = new GetRowRequest();
        PrimaryKey primaryKey1 = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("aa")))
                .build();
        SingleRowQueryCriteria singleRowQueryCriteria = new SingleRowQueryCriteria(CST);
        singleRowQueryCriteria.setPrimaryKey(primaryKey1);
        getRowRequest.setRowQueryCriteria(singleRowQueryCriteria);
        GetRowResponse response = syncClient.getRow(getRowRequest);

        Assert.assertEquals(response.getRow().getColumns().length, 3);
        Assert.assertEquals(response.getRow().getLatestColumn("co1").getValue().asBoolean(), true);

        Assert.assertEquals(response.getRow().getLatestColumn("co2").getValue().asString(), "abbb");

        Assert.assertEquals(response.getRow().getLatestColumn("co3").getValue().asLong(), 13445);
    }


    @Test
    public void testPutAgainRow() {
        createTable();

        PutRowRequest putRowRequest = new PutRowRequest();
        PrimaryKey primaryKey = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("aa")))
                .build();
        RowPutChange change = new RowPutChange(CST,
                primaryKey);

        change.addColumn(new Column("co1", ColumnValue.fromBoolean(true)));
        change.addColumn(new Column("co2", ColumnValue.fromString("abbb")));
        change.addColumn(new Column("co3", ColumnValue.fromLong(13445)));
        putRowRequest.setRowChange(change);
        syncClient.putRow(putRowRequest);

        Assert.assertEquals(simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance
                .get(primaryKey)
                .getLatestColumn("co3").getValue().asLong(), 13445);


        PutRowRequest putRowRequest2 = new PutRowRequest();
        RowPutChange change2 = new RowPutChange(CST,
                primaryKey);
        change2.addColumn(new Column("co3", ColumnValue.fromString("aaaEx")));
        putRowRequest2.setRowChange(change2);
        syncClient.putRow(putRowRequest2);

        Assert.assertEquals(simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance
                .get(primaryKey)
                .getLatestColumn("co1").getValue().asBoolean(), true);

        Assert.assertEquals(simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance
                .get(primaryKey)
                .getLatestColumn("co2").getValue().asString(), "abbb");

        System.out.println("column3Value:" + simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance
                .get(primaryKey)
                .getLatestColumn("co3").getValue());

        System.out.println("fullData:" + simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance.toString());


        Assert.assertEquals(simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance
                .get(primaryKey)
                .getLatestColumn("co3").getValue().asString(), "aaaEx");

        Assert.assertEquals(simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance
                .get(primaryKey).getColumn("co3").size(), 2);
    }

    @Test
    public void testUpdateRow() {
        createTable();
        PutRowRequest putRowRequest = new PutRowRequest();
        PrimaryKey primaryKey = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("aa")))
                .build();
        RowPutChange change = new RowPutChange(CST,
                primaryKey);
        long timestamp = System.nanoTime();
        change.addColumn(new Column("co1", ColumnValue.fromBoolean(true)));
        change.addColumn(new Column("co2", ColumnValue.fromString("abbb"), timestamp));
        change.addColumn(new Column("co3", ColumnValue.fromLong(13445)));
        putRowRequest.setRowChange(change);
        syncClient.putRow(putRowRequest);


        UpdateRowRequest updateRowRequest = new UpdateRowRequest();
        RowUpdateChange rowUpdateChange = new RowUpdateChange(CST, primaryKey);

        rowUpdateChange.deleteColumns("co1");

        // 删除某列的某一版本
        rowUpdateChange.deleteColumn("co2", timestamp);

        rowUpdateChange.put(new Column("co3", ColumnValue.fromString("siemens")));

        updateRowRequest.setRowChange(rowUpdateChange);

        syncClient.updateRow(updateRowRequest);

        PrimaryKey primaryKey1 = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("aa")))
                .build();
        SingleRowQueryCriteria singleRowQueryCriteria = new SingleRowQueryCriteria(CST);
        singleRowQueryCriteria.setPrimaryKey(primaryKey1);
        GetRowRequest getRowRequest = new GetRowRequest();
        getRowRequest.setRowQueryCriteria(singleRowQueryCriteria);
        GetRowResponse response = syncClient.getRow(getRowRequest);
        System.out.println();
        System.out.println("fullData:" + simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance.toString());

        Assert.assertEquals(1, response.getRow().getColumns().length);
        Assert.assertNull(response.getRow().getLatestColumn("co1"));
        Assert.assertNull(response.getRow().getLatestColumn("co2"));
        Assert.assertEquals(response.getRow().getColumn("co2").size(), 0);
        Assert.assertEquals("siemens", response.getRow().getLatestColumn("co3").getValue().asString());
    }


    @Test
    public void testUpdateRowWithVersion() {
        createTable();
        PutRowRequest putRowRequest = new PutRowRequest();
        PrimaryKey primaryKey = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("aa")))
                .build();
        RowPutChange change = new RowPutChange(CST,
                primaryKey);
        long timestamp = System.nanoTime();
        change.addColumn(new Column("co1", ColumnValue.fromBoolean(true)));
        change.addColumn(new Column("co2", ColumnValue.fromString("abbb"), timestamp));
        change.addColumn(new Column("co3", ColumnValue.fromLong(13445)));
        putRowRequest.setRowChange(change);

        syncClient.putRow(putRowRequest);
        UpdateRowRequest updateRowRequest = new UpdateRowRequest();
        RowUpdateChange rowUpdateChange = new RowUpdateChange(CST, primaryKey);

        rowUpdateChange.put(new Column("co2", ColumnValue.fromString("siemens"), timestamp));

        updateRowRequest.setRowChange(rowUpdateChange);

        syncClient.updateRow(updateRowRequest);

        PrimaryKey primaryKey1 = PrimaryKeyBuilder
                .createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("aa")))
                .build();
        SingleRowQueryCriteria singleRowQueryCriteria = new SingleRowQueryCriteria(CST);
        singleRowQueryCriteria.setPrimaryKey(primaryKey1);
        GetRowRequest getRowRequest = new GetRowRequest();
        getRowRequest.setRowQueryCriteria(singleRowQueryCriteria);
        GetRowResponse response = syncClient.getRow(getRowRequest);
        System.out.println();
        System.out.println("fullData:" + simpleInMemoryTableStore
                .getInMemoryTableInstance(CST)
                .dataInstance.toString());


        Assert.assertEquals(true, response.getRow().getLatestColumn("co1").getValue().asBoolean());
        Assert.assertEquals("siemens", response.getRow().getLatestColumn("co2").getValue().asString());
        Assert.assertEquals(13445, response.getRow().getLatestColumn("co3").getValue().asLong());
    }
}
