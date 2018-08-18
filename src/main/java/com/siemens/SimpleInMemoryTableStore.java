package com.siemens;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 要求metas所有的表名不能重复
 * 管理许多内存TableStore数据库表,
 * 增删改查的数据都在数据库表中
 */
public class SimpleInMemoryTableStore {

    /**
     * 所有的表
     */
    List<TableMeta> metas = new ArrayList<>();

    /**
     * 所有的表名
     * key TableName
     * value TableMeta
     */
    Map<String, TableMeta> tableMetaMap = new HashMap<>();


    public Map<String, InMemoryTableInstance> getMetaInMemoryTableInstances() {
        return metaInMemoryTableInstances;
    }

    /**
     * 所有的表的内存实例
     * Key TableName
     * value stored data.
     */
    Map<String, InMemoryTableInstance> metaInMemoryTableInstances = new HashMap<>();


    public SimpleInMemoryTableStore(){

    }

    public List<TableMeta> getMetas() {
        return metas;
    }

    public Map<String, TableMeta> getTableMetaMap() {
        return tableMetaMap;
    }

    public void addTableMeta(TableMeta meta) {
        Preconditions.checkArgument(meta != null, "The tablemeta should not be null.");
        this.getMetas().add(meta);
        this.getTableMetaMap() .put(meta.getTableName(), meta);
        this.getMetaInMemoryTableInstances().put(meta.getTableName(), new InMemoryTableInstance(meta));
    }

    public TableMeta getTableMeta(String tableName) {
        Preconditions.checkArgument(tableName != null && !"".equals(tableName), "The name of table name should not be null or empty.");
        return this.getTableMetaMap().get(tableName);
    }

    public InMemoryTableInstance getInMemoryTableInstance(String tableName) {
        Preconditions.checkArgument(tableName != null && !"".equals(tableName), "The name of table name should not be null or empty.");
        return this.getMetaInMemoryTableInstances().get(tableName);
    }


    public UpdateRowResponse noConditionUpdateRow(UpdateRowRequest updateRowRequest) {
        String tableName = updateRowRequest.getRowChange().getTableName();
        return this.getMetaInMemoryTableInstances().get(tableName).noConditionUpdateRow(updateRowRequest);
    }

    public PutRowResponse noConditionPutRow(PutRowRequest putRowRequest) {
        String tableName = putRowRequest.getRowChange().getTableName();
        return this.getMetaInMemoryTableInstances().get(tableName).noConditionPutRow(putRowRequest);
    }

    public GetRowResponse noConditionGetRow(GetRowRequest getRowRequest) {
        String tableName = getRowRequest.getRowQueryCriteria().getTableName();
        return this.getMetaInMemoryTableInstances().get(tableName).noConditionGetRow(getRowRequest);
    }

    public CreateTableResponse createTable(CreateTableRequest createTableRequest)
            throws TableStoreException, ClientException{
        TableMeta tableMeta = createTableRequest.getTableMeta();
        if(this.getMetas().contains(tableMeta)){
            throw new TableStoreException("ObjectAlreadyExist", new RuntimeException("Requested table already exists."), "409", "0", 409);
        }
        else{
            this.addTableMeta(tableMeta);
        }
        return new CreateTableResponse(new Response());
    }

    public DeleteTableResponse deleteTable(DeleteTableRequest deleteTableRequest)
            throws TableStoreException, ClientException{
        String tableName = deleteTableRequest.getTableName();
        if(!this.getTableMetaMap().containsKey(tableName)){
            throw new TableStoreException("OTSObjectNotExist", new RuntimeException("Requested table does not exist."), "404", "0", 404);
        }
        else{
            this.clean(tableName);
        }
        return new DeleteTableResponse(new Response());
    }

    public void clean(String tableName){

        TableMeta meta = this.tableMetaMap.get(tableName);
        this.metas.remove(meta);
        this.tableMetaMap.remove(tableName);
        this.metaInMemoryTableInstances.remove(tableName);
    }

    public void clean(){
        this.metas.clear();
        this.tableMetaMap.clear();
        this.metaInMemoryTableInstances.clear();
    }
}
