package com.aihuishou.bi.sync;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface LongReadFunction extends ReadFunction {

    @Override
    default Class<?> getJavaType() {
        return long.class;
    }

    long readLong(ResultSet resultSet, int columnIndex) throws SQLException;
}
