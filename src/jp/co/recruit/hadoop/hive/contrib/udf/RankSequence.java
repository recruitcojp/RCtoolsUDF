/*
 * RankSequence.java
 *
 * Version 0.1 2011/01/25 新規作成(Hive0.7.0より移植)
 * Version 0.2 2011/03/02 コメント追加
 * Version 0.3 2011/03/28 RowSequenceを改変しRank関数を作成
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
 * @version 0.3
 * @author Masanori OTSUBO &lt;masa_otsubo@waku-2.com&gt;
 * @author Takahiro SHIMAZAKI &lt;shimazaki@waku-2.com&gt;
 */
@Description(name = "rank_sequence", value = "_FUNC_(KEY) - "
    + "Returns a generated sequence number starting from 1 about each KEY")
@UDFType(deterministic = false)
public class RankSequence extends UDF {

    /** シーケンス番号 */
    private LongWritable result = new LongWritable();

    /** 一つ前のキーを保持 */
    private String preStr;

    /**
     * コンストラクタ
     */
    public RankSequence() {
        resetResult();
        preStr = "";
    }

    /**
     * シーケンス番号をリセット
     */
    private void resetResult() {
        result.set(0L);
    }

    /**
     * フィールドの値を1プラスしているだけ。<br>
     * インスタンスが複数生成されると、Rowナンバーは一意でなくなるので注意<br>
     * また、引数で与えたキーが変わると1にリセットされる。
     * 
     * @param str
     * @return
     */
    public LongWritable evaluate(final String str) {

        if (!str.equals(preStr)) {
            preStr = str;
            resetResult();
        }

        result.set(result.get() + 1L);

        return result;
    }
}
