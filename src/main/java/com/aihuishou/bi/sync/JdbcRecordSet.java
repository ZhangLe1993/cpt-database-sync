package com.aihuishou.bi.sync;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.util.List;

public class JdbcRecordSet implements RecordSet {
    private List<Type> columnTypes;
    private List<JdbcColumnHandle> columnHandles;
    private MetaInfo metaInfo;
    private Connection connection;

    public JdbcRecordSet(MetaInfo metaInfo, List<JdbcColumnHandle> columnHandles, Connection connection) {
        this.metaInfo = metaInfo;
        this.columnHandles = columnHandles;
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (JdbcColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.connection = connection;
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new JdbcRecordCursor(metaInfo, columnHandles, connection);
    }
}
