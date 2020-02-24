package com.aihuishou.bi.sync;

import com.facebook.presto.spi.Page;
import com.aihuishou.bi.utils.Utils;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class M2H2DataMigrator {
    private MetaInfo metaInfo;
    private List<JdbcColumnHandle> columnHandles;
    private BlockingQueue<Page> queue;

    public M2H2DataMigrator(MetaInfo metaInfo, List<JdbcColumnHandle> columnHandleList, BlockingQueue<Page> queue) {
        this.metaInfo = metaInfo;
        this.columnHandles = columnHandleList;
        this.queue = queue;
    }

    public void transferTable(DataSource sourceDataSource, DataSource targetDataSource) throws SQLException, InterruptedException {
        System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " Transfer table called");
        FetchRunner fetchRunner = new FetchRunner(sourceDataSource, metaInfo, columnHandles, queue);
        fetchRunner.setName("fetchRunner");
        H2Runner h2Runner = new H2Runner(targetDataSource, metaInfo, columnHandles, queue);
        h2Runner.setName("h2Runner");
        fetchRunner.start();
        while (fetchRunner.isAlive() && queue.isEmpty()) {
            Thread.sleep(100);
        }
        h2Runner.start();
        fetchRunner.join();
        h2Runner.join();
        System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " Transfer table end");
    }
}
