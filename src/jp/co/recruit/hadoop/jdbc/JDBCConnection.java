/*
 * JDBCConnection.java
 *
 * Copyright (C) 2011 RECRUIT Corporation, All Rights Reserved.
 * RECRUIT Corporation CONFIDENTIAL.
 */
package jp.co.recruit.hadoop.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;

/**
 * <p>
 * JDBC接続用コネクションプール
 * </p>
 * 
 * 接続方法には以下の手法がある。
 * <ul>
 * <li>外部ファイル「jdbc.properties」に定義を行う。
 * <li>サーバ情報を持つJDBCServerPropertyのインスタンスを使用する。
 * </ul>
 */
public class JDBCConnection {
    /** JDBC設定ファイル */
    protected static final String PROP_FILE_NAME = "jdbc.properties";

    /** コネクションプール用データソース */
    protected final PoolingDataSource dataSource;

    /**
     * テスト用サンプル
     * 
     * @param args
     * @throws ParseException
     */
    public static void main(String[] args) throws ParseException {
        // インスタンスの生成
        JDBCConnection conn = new JDBCConnection();

        // SQLの発行と、Iteratorの取得
        ResultIterator iterator = conn.select("select * from sample");

        while (iterator.hasNext()) {
            Map<String, Object> map = iterator.next();
            for (String key : map.keySet()) {
                System.out.print(key + ":" + map.get(key) + "\t");
            }
            System.out.print("\n");
        }
    }

    public JDBCConnection() {
        this((JDBCServerProperty) null);
    }

    public JDBCConnection(String props) throws ParseException {
        this(new OracleProperty(props));
    }

    /**
     * 
     * @param prop サーバ接続プロパティ
     */
    public JDBCConnection(JDBCServerProperty prop) {
        Properties p = new Properties();

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(
            PROP_FILE_NAME);
        if (is != null) {
            try {
                p.load(is);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (prop == null) {
            prop = new OracleProperty(p);
        }

        ObjectPool pool = null;
        try {
            Class.forName(p.getProperty("driver") != null ? p.getProperty("driver")
                : OracleProperty.DRIVER);
            SimpleConnectionFactory factory = new SimpleConnectionFactory(prop.getUrl(), prop
                .getUser(), prop.getPassword());
            String poolMax, poolInit;

            if ((poolMax = p.getProperty("pool.max")) == null) {
                pool = new StackObjectPool(factory);
            } else if ((poolInit = p.getProperty("pool.init")) == null) {
                pool = new StackObjectPool(factory, Integer.parseInt(poolMax));
            } else {
                pool = new StackObjectPool(factory, Integer.parseInt(poolMax), Integer
                    .parseInt(poolInit));
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        dataSource = new PoolingDataSource(pool);
    }

    private Connection getConnection() throws Exception {
        return dataSource.getConnection();
    }

    private void returnConnection(Connection conn) throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * 
     * @param sql SQL文
     * @return 結果Iterator
     */
    public ResultIterator select(String sql) {
        return new ResultIterator(sql, this);
    }

    /**
     * DBからの結果をIterator形式で扱うクラス。
     */
    public static class ResultIterator {
        private final JDBCConnection pool;
        private ResultSet resultSet;
        private Connection conn;
        private Statement stmt;
        private final Map<String, Integer> columns;
        private final Iterator<Map<String, Object>> rSetIterator;
        private Map<String, Object> pointer = null;
        private static final Iterator<Map<String, Object>> EMPTY_ITERATOR = new ArrayList<Map<String, Object>>()
            .iterator();
        private boolean hasnext = false;

        public ResultIterator(String sql, JDBCConnection pool) {
            this.pool = pool;
            try {
                conn = pool.getConnection();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            try {
                stmt = conn
                    .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                if (stmt.execute(sql)) {
                    resultSet = stmt.getResultSet();
                    columns = readFields(resultSet.getMetaData());
                } else {
                    resultSet = null;
                    columns = null;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (resultSet == null) {
                rSetIterator = EMPTY_ITERATOR;
                return;
            }
            rSetIterator = new Iterator<Map<String, Object>>() {
                public boolean hasNext() {
                    return hasnext = hasnext == false ? hasnext() : true;
                }

                public Map<String, Object> next() {
                    if (hasnext) {
                        hasnext = false;
                        return getARow();
                    }
                    return null;
                }

                public void remove() {
                } /* do nothing */
            };
        }

        private Map<String, Integer> readFields(ResultSetMetaData metaData) throws SQLException {
            Map<String, Integer> columns = new HashMap<String, Integer>();
            int count = metaData.getColumnCount();
            for (int i = 1; i <= count; i++) {
                columns.put(metaData.getColumnLabel(i), metaData.getColumnType(i));
            }
            return columns;
        }

        public Map<String, Object> getCurrentValue() {
            return pointer;
        }

        public Iterator<Map<String, Object>> getIterator() {
            return rSetIterator;
        }

        public boolean hasNext() {
            return rSetIterator.hasNext();
        }

        public Map<String, Object> next() {
            return pointer = rSetIterator.hasNext() ? rSetIterator.next() : null;
        }

        private Map<String, Object> getARow() {
            if (resultSet == null) return null;
            Map<String, Object> result = new HashMap<String, Object>();

            for (Iterator<String> it = columns.keySet().iterator(); it.hasNext();) {
                String colName = it.next();
                try {
                    Integer type = columns.get(colName);
                    if (type == null) type = Types.VARCHAR;
                    switch (type) {
                    case Types.INTEGER:
                        result.put(colName, resultSet.getInt(colName));
                        break;
                    case Types.FLOAT:
                        result.put(colName, resultSet.getFloat(colName));
                        break;
                    case Types.BIGINT:
                        result.put(colName, resultSet.getLong(colName));
                        break;
                    case Types.DOUBLE:
                        result.put(colName, resultSet.getDouble(colName));
                        break;
                    case Types.DATE:
                        result.put(colName, resultSet.getDate(colName));
                        break;
                    case Types.BOOLEAN:
                        result.put(colName, resultSet.getBoolean(colName));
                        break;
                    case Types.BLOB:
                        result.put(colName, resultSet.getBytes(colName));
                        break;
                    case Types.CHAR: {
                        String str = resultSet.getString(colName);
                        result.put(colName, str == null ? str : str.trim());
                        break;
                    }
                    default:
                        result.put(colName, resultSet.getString(colName));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            return result;
        }

        private boolean hasnext() {
            if (resultSet == null) return false;
            try {
                if (resultSet.next()) {
                    return true;
                } else {
                    close();
                    return false;
                }
            } catch (SQLException e) {
                close();
                return false;
            }
        }

        private void close() {
            try {
                if (resultSet != null) resultSet.close();
                if (stmt != null) {
                    stmt.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                resultSet = null;
                stmt = null;
                try {
                    if (conn != null) pool.returnConnection(conn);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    conn = null;
                }
            }
        }
    }
}
