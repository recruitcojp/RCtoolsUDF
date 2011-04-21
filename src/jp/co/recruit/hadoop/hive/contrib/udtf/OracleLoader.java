/*
 * OracleLoader.java
 *
 * Version 0.1 2011/03/08 新規作成
 * Version 0.2 2011/03/10 コメント追加
 *
 * Copyright (C) 2011 RECRUIT Corporation, All Rights Reserved.
 * RECRUIT Corporation CONFIDENTIAL.
 */
package jp.co.recruit.hadoop.hive.contrib.udtf;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jp.co.recruit.hadoop.jdbc.JDBCConnection;
import jp.co.recruit.hadoop.jdbc.JDBCConnection.ResultIterator;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Hive上でSQLを記述しOracleからデータを抽出するUDTF<br>
 * 
 * @version 0.1
 * @author Minoru EGUCHI &lt;minoru_eguchi@waku-2.com&gt;
 * @author Masanori OTSUBO &lt;masa_otsubo@waku-2.com&gt;
 */
@Description(name = "oracle_loader", value = "_FUNC_(arg1, arg2, arg3 ... argN) - "
    + "arg1 is a Server Information of Oracle DB. (ex. user/password@host:port/serviceName" + "\n"
    + "arg2 is a SQL Sentence of Oracle DB." + "\n" + "arg3 ... argN are column names that needed.")
public class OracleLoader extends GenericUDTF {

    /** [入力定義] 第1引数: Oracle接続情報 */
    private PrimitiveObjectInspector connOI = null;

    /** [入力定義] 第2引数: 実行するSQL文 */
    private PrimitiveObjectInspector sqlOI = null;

    /** [入力定義] 第3引数以降: 取得カラム */
    private PrimitiveObjectInspector[] columnsOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        // 引数チェック
        if (args.length < 3) {
            throw new UDFArgumentException(
                "Usage: oracle_loader(server, sql, columnName1...columnNameZ)");
        }

        int pos = 0;

        // [第1引数] Oracle接続情報
        connOI = OracleLoader.validatePrimitive(args[pos++], String.class);

        // [第2引数] 実行するSQL文
        sqlOI = OracleLoader.validatePrimitive(args[pos++], String.class);

        // 出力定義用フィールド
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        // [第3引数] 取得カラム
        columnsOI = new PrimitiveObjectInspector[args.length - pos];
        for (int i = 0; i < columnsOI.length; i++) {
            columnsOI[i] = OracleLoader.validatePrimitive(args[i + pos], String.class);
            fieldNames.add("column_" + (i + 1));
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        // Oracle接続情報の取得
        String server = (String) connOI.getPrimitiveJavaObject(args[0]);
        if (server.length() == 0) {
            server = null;
        }

        // SQL文の取得
        String sql = (String) sqlOI.getPrimitiveJavaObject(args[1]);
        if (sql == null || sql.length() == 0) {
            return;
        }

        // その他カラムの取得
        Set<String> columns = new LinkedHashSet<String>();
        for (int i = 0; i < columnsOI.length; i++) {
            // 取得カラム名をセット
            String wk = (String) columnsOI[i].getPrimitiveJavaObject(args[i + 2]);
            wk = wk.toUpperCase();
            columns.add(wk);
        }

        try {
            JDBCConnection conn = new JDBCConnection(server);
            ResultIterator result = conn.select(sql);
            while (result.hasNext()) {
                Map<String, Object> record = result.next();
                List<Object> forwardObj = new ArrayList<Object>(columns.size());

                for (String column : columns) {
                    forwardObj.add(record.get(column));
                }
                forward(forwardObj.toArray(new Object[forwardObj.size()]));
            }

        } catch (ParseException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public void close() throws HiveException {
        // 何もしない
    }

    @Override
    public String toString() {
        return "oracle_loader";
    }

    protected static <T> PrimitiveObjectInspector validatePrimitive(ObjectInspector oi, Class<T> c)
        throws UDFArgumentException {
        if (oi.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
            if (c == Boolean.class) {
                // Boolean
                if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN) {
                    return poi;
                }
            } else if (c == Integer.class) {
                // Integer
                if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
                    return poi;
                }
            } else if (c == Long.class) {
                // Long
                if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG) {
                    return poi;
                }
            } else if (c == Float.class) {
                // Float
                if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.FLOAT) {
                    return poi;
                }
            } else if (c == Double.class) {
                // Double
                if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
                    return poi;
                }
            } else if (c == String.class) {
                // String
                if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                    return poi;
                }
            }
        }
        throw new UDFArgumentException("Argument type is " + oi.getTypeName() + ". [" + c.getName()
            + "]");
    }
}
