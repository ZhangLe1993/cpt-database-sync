package com.aihuishou.bi.sync;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordPageSource;
import com.aihuishou.bi.utils.Utils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;


public class FetchRunner extends Thread {
    private Connection connection;
    private BlockingQueue<Page> queue;
    private RecordPageSource pageSource;

    public FetchRunner(DataSource dataSource, MetaInfo metaInfo, List<JdbcColumnHandle> columnHandles, BlockingQueue<Page> queue) {
        this.queue = queue;
        try {
            connection = dataSource.getConnection();
            pageSource = new RecordPageSource(new JdbcRecordSet(metaInfo, columnHandles, connection));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        int conNum = 0;
        try {
            Page page = pageSource.getNextPage();
            while (!pageSource.isFinished()) {
                if (page != null) {
                    queue.put(page);
                    conNum += page.getPositionCount();
                    System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " fetch count: " + conNum);
                }
                page = pageSource.getNextPage();
            }
            if (page != null) {
                queue.put(page);
                conNum += page.getPositionCount();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (pageSource != null) {
                pageSource.close();
            }
            Utils.close(connection, null, null);
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " final fetch count: " + conNum);
        }
    }

    public void shutdown() {
    }
}
