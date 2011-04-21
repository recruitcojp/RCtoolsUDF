/*
 * StringExploder.java
 *
 * Version 0.1 2011/02/28 新規作成
 * Version 0.2 2011/03/01 区切り文字を指定できるように変更
 * Version 0.3 2011/03/01 他のカラムを同時出力できるように変更
 *
 * Copyright (C) 2011 RECRUIT Corporation, All Rights Reserved.
 * RECRUIT Corporation CONFIDENTIAL.
 */
package jp.co.recruit.hadoop.hive.contrib.udtf;

import java.util.ArrayList;

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
 * 文字列を展開するUDTF <br>
 * <br>
 * 第1引数に区切り文字、第2引数に展開対象カラムを指定すると、区切り文字に従い展開する。<br>
 * 第3引数以降に、他のカラムを指定すると、そのまま出力する。<br>
 * 
 * @version 0.3
 * @author Masanori OTSUBO &lt;masa_otsubo@waku-2.com&gt;
 */
@Description(name = "string_exploder", value = "_FUNC_(a, b, [c...]) - "
    + "a: delim (string), b: explode column (string), c: other column (string/int)")
public class StringExploder extends GenericUDTF {

    /** [入力定義] 第1引数: 区切り文字 */
    private PrimitiveObjectInspector delimOI = null;
    /** [入力定義] 第2引数: 展開対象となるカラム */
    private PrimitiveObjectInspector explodeColumnOI = null;
    /** [入力定義] 第3引数以降: その他の同時出力カラム */
    private PrimitiveObjectInspector[] otherColumnOI = null;

    /** [出力定義] 出力内容を格納するための領域 */
    private Object[] forwardObj = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        // 引数チェック
        if (args.length < 2) {
            throw new UDFArgumentException("Usage: string_explode(delim, explode column)");
        }

        // OI型チェック
        for (ObjectInspector oi : args) {
            if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentException(
                    "string_explode() takes an primitive as a parameter.");
            }
        }

        // 出力定義用フィールド
        forwardObj = new Object[args.length - 1];
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        // [第1引数] 区切り文字
        delimOI = (PrimitiveObjectInspector) args[0];
        if (delimOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("The delim isn't string.");
        }

        // [第2引数] 展開対象カラム
        explodeColumnOI = (PrimitiveObjectInspector) args[1];
        if (explodeColumnOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("The explode column isn't string.");
        }
        fieldNames.add("col_exp");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        // [第3引数以降] その他の同時出力カラム
        otherColumnOI = new PrimitiveObjectInspector[args.length - 2];
        for (int i = 0; i < otherColumnOI.length; i++) {
            otherColumnOI[i] = (PrimitiveObjectInspector) args[i + 2];
            fieldNames.add("col_" + (i + 1));
            switch (otherColumnOI[i].getPrimitiveCategory()) {
            case BOOLEAN:
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
                break;
            case BYTE:
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
                break;
            case DOUBLE:
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
                break;
            case FLOAT:
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
                break;
            case INT:
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
                break;
            case LONG:
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                break;
            case SHORT:
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
                break;
            case STRING:
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                break;
            default:
                throw new UDFArgumentException("The other column is an invalid type.");
            }
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        // 区切り文字の取得
        String delim = (String) delimOI.getPrimitiveJavaObject(args[0]);
        if (delim == null) {
            return;
        } else if (delim.length() == 0) {
            delim = ","; // デフォルトは、カンマ(,)。
        }

        // 展開カラムの取得
        String explodeColumn = (String) explodeColumnOI.getPrimitiveJavaObject(args[1]);
        if (explodeColumn == null || explodeColumn.length() == 0) {
            return; // 展開対象が無ければ、そのレコードは無視。
        }

        // その他カラムの取得
        for (int i = 0; i < otherColumnOI.length; i++) {
            // 直接フォワードObjに入れる。
            forwardObj[i + 1] = otherColumnOI[i].getPrimitiveJavaObject(args[i + 2]);
        }

        // 展開カラムの展開処理
        String[] elements = explodeColumn.split(delim);
        for (String e : elements) {
            forwardObj[0] = e;
            forward(forwardObj);
        }
    }

    @Override
    public void close() throws HiveException {
        // 何もしない
    }

    @Override
    public String toString() {
        return "string_exploder";
    }
}
