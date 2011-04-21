/*
 * Hash.java
 *
 * Version 0.1 2011/03/28 新規作成
 *
 * Copyright (C) 2011 RECRUIT Corporation, All Rights Reserved.
 * RECRUIT Corporation CONFIDENTIAL.
 */
package jp.co.recruit.hadoop.hive.contrib.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * 引数に与えられた文字列をハッシュ化する関数<br>
 * 
 * @version 0.1
 * @author Masanori OTSUBO &lt;masa_otsubo@waku-2.com&gt;
 */
@Description(name = "hash", value = "_FUNC_(str, [1]) - "
    + "Returns a hash code of the 1st argument string." + "\n"
    + "If there is no 2nd argument, the hash code is Integer." + "\n"
    + "If there is the 2nd argument, the hash code is Long.")
public class Hash extends UDF {

    /**
     * デフォルトのハッシュ関数を使用。
     * 
     * @param str ハッシュ化する文字列
     * @return Intのハッシュ値
     */
    public IntWritable evaluate(String str) {
        return new IntWritable(str.hashCode());
    }

    /**
     * Longでハッシュ値を取得。
     * 
     * @param str ハッシュ化する文字列
     * @param mode 任意の数字。オーバーロードするための引数
     * @return Longのハッシュ値
     */
    public LongWritable evaluate(String str, int mode) {
        return new LongWritable(hashCode(str));
    }

    /**
     * 文字列のLongハッシュ値を取得するメソッド
     * 
     * @param str
     * @return
     */
    private long hashCode(String str) {
        long h = 0L;
        char val[] = str.toCharArray();

        for (int i = 0; i < str.length(); i++) {
            h = 63 * h + val[i];
        }
        return h;
    }

    /**
     * テスト用メイン
     * 
     * @param args
     */
    public static void main(String[] args) {

        String str = "abclaslkjoihggugguuuuuuuuuguguggggggdk";

        Hash h = new Hash();

        System.out.println(str.hashCode());
        System.out.println(h.hashCode(str));

    }
}
