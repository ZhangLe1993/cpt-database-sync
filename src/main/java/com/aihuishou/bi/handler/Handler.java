package com.aihuishou.bi.handler;

import com.aihuishou.bi.sync.M2H2DataMigrator;
import com.aihuishou.bi.sync.M2H2SchemaConverter;
import com.aihuishou.bi.sync.MetaInfo;
import com.aihuishou.bi.utils.Utils;
import com.facebook.presto.spi.Page;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class Handler {

    @Resource(name = "sourceDataSource")
    private DataSource sourceDataSource;

    @Resource(name = "targetDataSource")
    private DataSource targetDataSource;

    public void fetch(String database) throws Exception {
        String sql = "select table_name from information_schema.tables where table_schema=?;";
        List<String> list = new QueryRunner(sourceDataSource).query(sql, new ColumnListHandler<>("table_name"), database);
        for(String table : list) {
            sync(database, database, table);
        }
    }

    /**
     * 将一个表强制拷贝到从库
     * @param sourceDb
     * @param targetDb
     * @param table
     * @throws Exception
     */
    public void sync(String sourceDb, String targetDb, String table) throws Exception {
        MetaInfo metaInfo = getMetaInfo(sourceDb, targetDb, table);
        M2H2SchemaConverter schemaConverter = new M2H2SchemaConverter(metaInfo);
        schemaConverter.getColumns(sourceDataSource);
        // 清空 H2分支表
        schemaConverter.truncateTable(targetDataSource);
        // 创建 H2分支表
        schemaConverter.createTableInTarget(targetDataSource);
        BlockingQueue<Page> queue = new LinkedBlockingQueue<>(1000);
        M2H2DataMigrator m2H2DataMigrator = new M2H2DataMigrator(metaInfo, schemaConverter.getColumnHandles(), queue);
        // 复制表数据 到 H2分支表
        m2H2DataMigrator.transferTable(sourceDataSource, targetDataSource);
        System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " Done\n");
    }

    private MetaInfo getMetaInfo(String sourceDb, String targetDb, String table) {
        MetaInfo metaInfo = new MetaInfo();
        // 源
        metaInfo.setMysqlDb(sourceDb);
        metaInfo.setTargetDb(targetDb);
        // 目标
        metaInfo.setTable(table);
        return metaInfo;
    }
}
