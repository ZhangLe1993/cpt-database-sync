package com.aihuishou.bi.sync;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface BooleanReadFunction extends ReadFunction {
    @Override
    default Class<?> getJavaType() {
        return boolean.class;
    }

    boolean readBoolean(ResultSet resultSet, int columnIndex) throws SQLException;
}
