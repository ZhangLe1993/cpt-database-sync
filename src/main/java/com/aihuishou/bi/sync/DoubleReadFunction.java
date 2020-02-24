package com.aihuishou.bi.sync;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface DoubleReadFunction extends ReadFunction {
    @Override
    default Class<?> getJavaType() {
        return double.class;
    }

    double readDouble(ResultSet resultSet, int columnIndex) throws SQLException;
}
