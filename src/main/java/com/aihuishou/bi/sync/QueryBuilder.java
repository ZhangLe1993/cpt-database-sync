package com.aihuishou.bi.sync;

import com.facebook.presto.spi.type.*;
import com.google.common.base.Joiner;
import com.aihuishou.bi.utils.Utils;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;
import static org.joda.time.DateTimeZone.UTC;

public class QueryBuilder {
    private final String quote;

    public QueryBuilder(String quote) {
        this.quote = requireNonNull(quote, "quote is null");
    }

    private static boolean isAcceptedType(Type type) {
        Type validType = requireNonNull(type, "type is null");
        return validType.equals(BigintType.BIGINT) ||
                validType.equals(TinyintType.TINYINT) ||
                validType.equals(SmallintType.SMALLINT) ||
                validType.equals(IntegerType.INTEGER) ||
                validType.equals(DoubleType.DOUBLE) ||
                validType.equals(RealType.REAL) ||
                validType.equals(BooleanType.BOOLEAN) ||
                validType.equals(DateType.DATE) ||
                validType.equals(TimeType.TIME) ||
                validType.equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE) ||
                validType.equals(TimestampType.TIMESTAMP) ||
                validType.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE) ||
                validType instanceof VarcharType;
    }

    public PreparedStatement buildSql(Connection connection, String catalog, String schema, String table, List<JdbcColumnHandle> columns) throws SQLException {
        StringBuilder sql = new StringBuilder();
        String columnNames = columns.stream()
                .map(JdbcColumnHandle::getColumnName)
                .map(this::quote)
                .collect(joining(", "));

        sql.append("SELECT ");
        sql.append(columnNames);
        if (columns.isEmpty()) {
            sql.append("null");
        }

        sql.append(" FROM ");
        if (!isNullOrEmpty(catalog)) {
            sql.append(quote(catalog)).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        sql.append(quote(table));

        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> clauses = Collections.emptyList();
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ").append(Joiner.on(" AND ").join(clauses));
        }

        PreparedStatement statement = Utils.getMysqlPreparedStatement(connection, sql.toString());

        for (int i = 0; i < accumulator.size(); i++) {
            TypeAndValue typeAndValue = accumulator.get(i);
            if (typeAndValue.getType().equals(BigintType.BIGINT)) {
                statement.setLong(i + 1, (long) typeAndValue.getValue());
            } else if (typeAndValue.getType().equals(IntegerType.INTEGER)) {
                statement.setInt(i + 1, ((Number) typeAndValue.getValue()).intValue());
            } else if (typeAndValue.getType().equals(SmallintType.SMALLINT)) {
                statement.setShort(i + 1, ((Number) typeAndValue.getValue()).shortValue());
            } else if (typeAndValue.getType().equals(TinyintType.TINYINT)) {
                statement.setByte(i + 1, ((Number) typeAndValue.getValue()).byteValue());
            } else if (typeAndValue.getType().equals(DoubleType.DOUBLE)) {
                statement.setDouble(i + 1, (double) typeAndValue.getValue());
            } else if (typeAndValue.getType().equals(RealType.REAL)) {
                statement.setFloat(i + 1, intBitsToFloat(((Number) typeAndValue.getValue()).intValue()));
            } else if (typeAndValue.getType().equals(BooleanType.BOOLEAN)) {
                statement.setBoolean(i + 1, (boolean) typeAndValue.getValue());
            } else if (typeAndValue.getType().equals(DateType.DATE)) {
                long millis = DAYS.toMillis((long) typeAndValue.getValue());
                statement.setDate(i + 1, new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millis)));
            } else if (typeAndValue.getType().equals(TimeType.TIME)) {
                statement.setTime(i + 1, new Time((long) typeAndValue.getValue()));
            } else if (typeAndValue.getType().equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE)) {
                statement.setTime(i + 1, new Time(unpackMillisUtc((long) typeAndValue.getValue())));
            } else if (typeAndValue.getType().equals(TimestampType.TIMESTAMP)) {
                statement.setTimestamp(i + 1, new Timestamp((long) typeAndValue.getValue()));
            } else if (typeAndValue.getType().equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
                statement.setTimestamp(i + 1, new Timestamp(unpackMillisUtc((long) typeAndValue.getValue())));
            } else if (typeAndValue.getType() instanceof VarcharType) {
                statement.setString(i + 1, ((Slice) typeAndValue.getValue()).toStringUtf8());
            } else {
                throw new UnsupportedOperationException("Can't handle type: " + typeAndValue.getType());
            }
        }

        return statement;
    }

    private String quote(String name) {
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }

    private static class TypeAndValue {
        private final Type type;
        private final Object value;

        public TypeAndValue(Type type, Object value) {
            this.type = requireNonNull(type, "type is null");
            this.value = requireNonNull(value, "value is null");
        }

        public Type getType() {
            return type;
        }

        public Object getValue() {
            return value;
        }
    }
}
