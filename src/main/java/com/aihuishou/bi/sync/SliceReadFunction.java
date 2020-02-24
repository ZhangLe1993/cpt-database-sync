package com.aihuishou.bi.sync;

import io.airlift.slice.Slice;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface SliceReadFunction extends ReadFunction
{
    @Override
    default Class<?> getJavaType()
    {
        return Slice.class;
    }

    Slice readSlice(ResultSet resultSet, int columnIndex) throws SQLException;
}
