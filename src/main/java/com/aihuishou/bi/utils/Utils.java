package com.aihuishou.bi.utils;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class Utils {

    private final static Logger logger = LoggerFactory.getLogger(Utils.class);

    public static volatile boolean fetchRunnerError;

    public static final String mysql_identifierQuote = "`";
    private static final DateFormat format1 = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");

    public static final Map<Type, String> SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "boolean")
            .put(BIGINT, "bigint")
            .put(INTEGER, "integer")
            .put(SMALLINT, "smallint")
            .put(TINYINT, "tinyint")
            .put(DOUBLE, "double precision")
            .put(REAL, "real")
            .put(VARBINARY, "varbinary")
            .put(DATE, "date")
            .put(TIME, "time")
            .put(TIME_WITH_TIME_ZONE, "time with timezone")
            .put(TIMESTAMP, "timestamp")
            .put(TIMESTAMP_WITH_TIME_ZONE, "timestamp with timezone")
            .build();

    public static void close(Connection connection, Statement statement, ResultSet resultSet) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                logger.error("", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.error("", e);
            }
        }
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                logger.error("", e);
            }
        }
    }

    public static Connection createConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public static String quoted(String catalog, String schema, String table, String identifierQuote) {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(quoted(catalog, identifierQuote)).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema, identifierQuote)).append(".");
        }
        sb.append(quoted(table, identifierQuote));
        return sb.toString();
    }

    public static String quoted(String schema, String table, String identifierQuote) {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema, identifierQuote)).append(".");
        }
        sb.append(quoted(table, identifierQuote));
        return sb.toString();
    }

    public static String quoted(String name, String identifierQuote) {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    public static String escapeNamePattern(String name, String escape) {
        if ((name == null) || (escape == null)) {
            return name;
        }
        checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        name = name.replace(escape, escape + escape);
        name = name.replace("_", escape + "_");
        name = name.replace("%", escape + "%");
        return name;
    }

    public static PreparedStatement getMysqlPreparedStatement(Connection connection, String sql) throws SQLException {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    public static String getCurrentTime() {
        return format1.format(new Date());
    }
}
