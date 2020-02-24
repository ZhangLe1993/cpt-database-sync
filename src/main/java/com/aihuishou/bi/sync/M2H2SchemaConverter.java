package com.aihuishou.bi.sync;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.aihuishou.bi.utils.Utils;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.aihuishou.bi.sync.StandardReadMappings.jdbcTypeToPrestoType;
import static com.aihuishou.bi.utils.Utils.mysql_identifierQuote;
import static com.aihuishou.bi.utils.Utils.quoted;
import static java.util.Locale.ENGLISH;

public class M2H2SchemaConverter {
    private MetaInfo metaInfo;
    private SchemaTableName mysqlTableHandle;
    private List<ColumnMetadata> columnMetadatas;
    private List<JdbcColumnHandle> columnHandles;

    public M2H2SchemaConverter(MetaInfo metaInfo) {
        this.metaInfo = metaInfo;
        mysqlTableHandle = new SchemaTableName(metaInfo.getMysqlDb(), metaInfo.getTable());
    }

    public List<JdbcColumnHandle> getColumnHandles() {
        return columnHandles;
    }

    public void truncateTable(DataSource targetDataSource) {
        String schema = metaInfo.getTargetDb();
        String tableName = metaInfo.getTable();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = targetDataSource.getConnection();
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.toUpperCase(ENGLISH);
                tableName = tableName.toUpperCase(ENGLISH);
            }
            StringBuilder sql = new StringBuilder()
                    .append("DROP TABLE IF EXISTS ")
                    .append(quoted(schema, tableName, mysql_identifierQuote))
                    .append(";");
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " sql: " + sql.toString());
            statement = connection.createStatement();
            statement.execute(sql.toString());
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Utils.close(connection, statement, resultSet);
        }
    }

    public void createTableInTarget(DataSource targetDataSource) {
        String schema = metaInfo.getTargetDb();
        String tableName = metaInfo.getTable();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = targetDataSource.getConnection();
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.toUpperCase(ENGLISH);
                tableName = tableName.toUpperCase(ENGLISH);
            }
            StringBuilder sql = new StringBuilder()
                    .append("CREATE TABLE IF NOT EXISTS ")
                    .append(quoted(schema, tableName, mysql_identifierQuote))
                    .append(" (");
            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : columnMetadatas) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(new StringBuilder()
                        .append(quoted(columnName, mysql_identifierQuote))
                        .append(" ")
                        .append(column.getType())
                        .toString());
            }
            Joiner.on(", ").appendTo(sql, columnList.build());
            sql.append(")");
            System.out.println(Utils.getCurrentTime() + " " + Thread.currentThread().getName() + " sql: " + sql.toString());
            statement = connection.createStatement();
            statement.execute(sql.toString());
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Utils.close(connection, statement, resultSet);
        }
    }

    public void getColumns(DataSource sourceDataSource) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = sourceDataSource.getConnection();
            //Utils.createConnection(metaInfo.getMysqlUrl(), metaInfo.getMysqlUser(), metaInfo.getMysqlPassword());
            resultSet = getColumns(connection.getMetaData());
            List<JdbcColumnHandle> columns = new ArrayList<>();
            while (resultSet.next()) {
                JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"));
                Optional<ReadMapping> columnMapping = toPrestoType(typeHandle);
                if (columnMapping.isPresent()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    columns.add(new JdbcColumnHandle(metaInfo.getMysqlDb(), columnName, typeHandle, columnMapping.get().getType()));
                }
            }
            if (columns.isEmpty()) {
                throw new TableNotFoundException(mysqlTableHandle);
            }
            columnHandles = columns;
            ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
            for (JdbcColumnHandle column : columns) {
                columnMetadata.add(column.getColumnMetadata());
            }
            columnMetadatas = columnMetadata.build();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Utils.close(connection, statement, resultSet);
        }
    }

    public Optional<ReadMapping> toPrestoType(JdbcTypeHandle typeHandle) {
        return jdbcTypeToPrestoType(typeHandle);
    }

    private ResultSet getColumns(DatabaseMetaData metadata) throws SQLException {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                null,
                Utils.escapeNamePattern(metaInfo.getMysqlDb(), escape),
                Utils.escapeNamePattern(metaInfo.getTable(), escape),
                null);
    }

    private String toMysqlSqlType(Type type) {
        if (REAL.equals(type)) {
            return "float";
        }
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        if (TIMESTAMP.equals(type)) {
            return "datetime";
        }
        if (VARBINARY.equals(type)) {
            return "mediumblob";
        }
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return "longtext";
            }
            if (varcharType.getLengthSafe() <= 255) {
                return "tinytext";
            }
            if (varcharType.getLengthSafe() <= 65535) {
                return "text";
            }
            if (varcharType.getLengthSafe() <= 16777215) {
                return "mediumtext";
            }
            return "longtext";
        }

        return toSqlType(type);
    }

    private String toSqlType(Type type) {
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return "varchar";
            }
            return "varchar(" + varcharType.getLengthSafe() + ")";
        }
        if (type instanceof CharType) {
            if (((CharType) type).getLength() == CharType.MAX_LENGTH) {
                return "char";
            }
            return "char(" + ((CharType) type).getLength() + ")";
        }
        if (type instanceof DecimalType) {
            return String.format("decimal(%s, %s)", ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
        }

        String sqlType = Utils.SQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }
}
