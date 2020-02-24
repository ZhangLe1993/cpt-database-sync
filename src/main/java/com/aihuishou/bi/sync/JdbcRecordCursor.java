package com.aihuishou.bi.sync;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.VerifyException;
import com.aihuishou.bi.utils.Utils;
import io.airlift.slice.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class JdbcRecordCursor implements RecordCursor {
    private final static Logger logger = LoggerFactory.getLogger(JdbcRecordCursor.class);

    private JdbcColumnHandle[] columnHandles;
    private BooleanReadFunction[] booleanReadFunctions;
    private DoubleReadFunction[] doubleReadFunctions;
    private LongReadFunction[] longReadFunctions;
    private SliceReadFunction[] sliceReadFunctions;
    private ResultSet resultSet;
    private Connection connection;
    private MetaInfo metaInfo;
    private PreparedStatement statement;
    private boolean closed;

    public JdbcRecordCursor(MetaInfo metaInfo, List<JdbcColumnHandle> columnHandles, Connection connection) {
        this.columnHandles = columnHandles.toArray(new JdbcColumnHandle[0]);
        this.metaInfo = metaInfo;
        booleanReadFunctions = new BooleanReadFunction[columnHandles.size()];
        doubleReadFunctions = new DoubleReadFunction[columnHandles.size()];
        longReadFunctions = new LongReadFunction[columnHandles.size()];
        sliceReadFunctions = new SliceReadFunction[columnHandles.size()];
        initColumns(columnHandles);
        try {
            this.connection = connection;
            this.connection.setAutoCommit(false);
            statement = buildSql(columnHandles);
            statement.setFetchSize(1000);
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " Executing: " + statement.toString());
            resultSet = statement.executeQuery();
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    public PreparedStatement buildSql(List<JdbcColumnHandle> columnHandles) throws SQLException {
        return new QueryBuilder(Utils.mysql_identifierQuote).buildSql(
                connection,
                null,
                null,
                metaInfo.getTable(),
                columnHandles);
    }

    protected void initColumns(List<JdbcColumnHandle> columnHandles) {
        for (int i = 0; i < this.columnHandles.length; i++) {
            Class<?> javaType = null;
            ReadFunction readFunction = null;
            try {
                ReadMapping readMapping = toPrestoType(columnHandles.get(i).getJdbcTypeHandle())
                        .orElseThrow(() -> new VerifyException("Unsupported column type"));
                javaType = readMapping.getType().getJavaType();
                readFunction = readMapping.getReadFunction();
            } catch (VerifyException e) {
                System.err.print("Unsupported column type,column info " + columnHandles.get(i));
                throw e;
            }
            initReadFunctions(javaType, readFunction, i);
        }
    }

    public Optional<ReadMapping> toPrestoType(JdbcTypeHandle typeHandle) {
        return StandardReadMappings.jdbcTypeToPrestoType(typeHandle);
    }

    protected void initReadFunctions(Class<?> javaType, ReadFunction readFunction, int i) {
        if (javaType == boolean.class) {
            booleanReadFunctions[i] = (BooleanReadFunction) readFunction;
        } else if (javaType == double.class) {
            doubleReadFunctions[i] = (DoubleReadFunction) readFunction;
        } else if (javaType == long.class) {
            longReadFunctions[i] = (LongReadFunction) readFunction;
        } else if (javaType == Slice.class) {
            sliceReadFunctions[i] = (SliceReadFunction) readFunction;
        } else {
            throw new IllegalStateException(format("Unsupported java type %s", javaType));
        }
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        return columnHandles[field].getColumnType();
    }

    @Override
    public boolean advanceNextPosition() {
        if (closed) {
            return false;
        }

        try {
            return resultSet.next();
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean getBoolean(int field) {
        checkState(!closed, "cursor is closed");
        try {
            return booleanReadFunctions[field].readBoolean(resultSet, field + 1);
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getLong(int field) {
        checkState(!closed, "cursor is closed");
        try {
            return longReadFunctions[field].readLong(resultSet, field + 1);
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public double getDouble(int field) {
        checkState(!closed, "cursor is closed");
        try {
            return doubleReadFunctions[field].readDouble(resultSet, field + 1);
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Slice getSlice(int field) {
        checkState(!closed, "cursor is closed");
        try {
            return sliceReadFunctions[field].readSlice(resultSet, field + 1);
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        checkState(!closed, "cursor is closed");
        checkArgument(field < columnHandles.length, "Invalid field index");

        try {
            resultSet.getObject(field + 1);
            return resultSet.wasNull();
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try (Connection connection = this.connection;
             Statement statement = this.statement;
             ResultSet resultSet = this.resultSet) {
            abortReadConnection(connection);
        } catch (SQLException e) {
            logger.error("", e);
        }
    }

    private void abortReadConnection(Connection connection) throws SQLException {
    }

    private RuntimeException handleSqlException(Exception e) {
        try {
            close();
        } catch (Exception closeException) {
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
        return new RuntimeException("jdbc error", e);
    }
}
