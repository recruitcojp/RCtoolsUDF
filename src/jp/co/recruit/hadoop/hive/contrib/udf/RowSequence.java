/*
 * RowSequence.java
 *
 * Version 0.1 2011/01/25 新規作成(Hive0.7.0より移植)
 * Version 0.2 2011/03/02 コメント追加
 *
 * Copyright (C) 2011 RECRUIT Corporation, All Rights Reserved.
 * RECRUIT Corporation CONFIDENTIAL.
 */
package jp.co.recruit.hadoop.hive.contrib.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.LongWritable;

/**
 * レコードに対してシーケンス番号を付与するUDF <br>
 * <br>
 * 全レコードに対して一意の番号を振りたい場合は、Mapタスク数を1に設定する必要がある。<br>
 * hive> SET mapred.map.tasks=1;<br>
 * 但し、Mapタスク数を1にすると<br>
 * 
 * @version 0.2
 * @author Takashi MORITSU &lt;t_moritsu@waku-2.com&gt;
 * @author Masanori OTSUBO &lt;masa_otsubo@waku-2.com&gt;
 */
@Description(name = "row_sequence", value = "_FUNC_() - "
    + "Returns a generated row sequence number starting from 1")
@UDFType(deterministic = false)
public class RowSequence extends UDF {

    /** インスタンスフィールド */
    private LongWritable result = new LongWritable();

    /**
     * インスタンスフィールドの値を0にセットする。
     */
    public RowSequence() {
        result.set(0L);
    }

    /**
     * フィールドの値を1プラスしているだけ。<br>
     * インスタンスが複数生成されると、Rowナンバーは一意でなくなるので注意<br>
     * 
     * @return
     */
    public LongWritable evaluate() {
        result.set(result.get() + 1L);
        return result;
    }
}
