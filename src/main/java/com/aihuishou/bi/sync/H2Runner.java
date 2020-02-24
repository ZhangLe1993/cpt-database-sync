package com.aihuishou.bi.sync;

import com.facebook.presto.spi.Page;
import com.aihuishou.bi.utils.Utils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;


public class H2Runner extends Thread {
    private BlockingQueue<Page> queue;
    private Connection connection;
    private JdbcPageSink jdbcPageSink;

    public H2Runner(DataSource targetDataSource, MetaInfo metaInfo, List<JdbcColumnHandle> columnHandles, BlockingQueue<Page> queue) {
        this.queue = queue;
        try {
            connection = targetDataSource.getConnection();
            jdbcPageSink = new JdbcPageSink(metaInfo, columnHandles, connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        int proNum = 0;
        try {
            Page page = null;
            while (!Utils.fetchRunnerError && !queue.isEmpty()) {
                page = queue.take();
                if (page != null) {
                    jdbcPageSink.appendPage(page);
                    proNum += page.getPositionCount();
                    System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " pull count: " + proNum);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (jdbcPageSink != null) {
                jdbcPageSink.finish();
            }
            Utils.close(connection, null, null);
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " final pull count: " + proNum);
        }
    }

    public void shutdown() {
    }
}
