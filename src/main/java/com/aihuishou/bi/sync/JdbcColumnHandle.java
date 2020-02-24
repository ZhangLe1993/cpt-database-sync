package com.aihuishou.bi.sync;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class JdbcColumnHandle {
    private final String connectorId;
    private final String columnName;
    private final JdbcTypeHandle jdbcTypeHandle;
    private final Type columnType;

    public JdbcColumnHandle(String connectorId, String columnName, JdbcTypeHandle jdbcTypeHandle, Type columnType) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.jdbcTypeHandle = requireNonNull(jdbcTypeHandle, "jdbcTypeHandle is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
    }

    public String getConnectorId() {
        return connectorId;
    }

    public String getColumnName() {
        return columnName;
    }

    public JdbcTypeHandle getJdbcTypeHandle() {
        return jdbcTypeHandle;
    }

    public Type getColumnType() {
        return columnType;
    }

    public ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(columnName, columnType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        JdbcColumnHandle o = (JdbcColumnHandle) obj;
        return Objects.equals(this.connectorId, o.connectorId) &&
                Objects.equals(this.columnName, o.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, columnName);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("columnName", columnName)
                .add("jdbcTypeHandle", jdbcTypeHandle)
                .add("columnType", columnType)
                .toString();
    }
}
