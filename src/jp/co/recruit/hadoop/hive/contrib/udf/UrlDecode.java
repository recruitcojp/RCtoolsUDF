/*
 * UrlDecode.java
 *
 * Version 0.1 2011/02/03 新規作成
 * Version 0.2 2011/03/10 コメント追加
 *
 * Copyright (C) 2011 RECRUIT Corporation, All Rights Reserved.
 * RECRUIT Corporation CONFIDENTIAL.
 */
package jp.co.recruit.hadoop.hive.contrib.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.codec.DecoderException;

/**
 * SC上にある様々な形式のURLを統一形式に変換するUDF<br>
 * 
 * @version 0.2
 * @author Takahiro SHIMAZAKI &lt;shimazaki@waku-2.com&gt;
 */
@Description(name = "url_decode ", value = "_FUNC_(string) - "
    + "Returns the decoded URL that is converted URL data on the SiteCatalyst")
public final class UrlDecode extends UDF {

    public Text evaluate(final String s) throws DecoderException {
        if (s == null) {
            return null;
        }
        String decodeTmp = "";
        String encodeTmp = "";

        java.util.Collection<String> list = new java.util.ArrayList<String>();
        list.add("utf-8");
        list.add("windows-31j");
        list.add("euc-jp");
        list.add("iso-2022-jp");

        // URLコードの修正. スペース文字を+に変換
        String org = s.replaceAll("%20", "+");
        org = org.replaceAll(" ", "+");

        URLCodec codec = new URLCodec();
        try {
            // 全ての文字コードでデコードし、結果が正しいものを返す
            for (String obj : list) {
                decodeTmp = codec.decode(s, obj);
                encodeTmp = codec.encode(decodeTmp, obj);

                if (org.equals(encodeTmp)) {
                    decodeTmp = decodeTmp.replaceAll("[\r|\n|\t]", " ");
                    return new Text(decodeTmp);
                }
            }

            // 大文字、小文字の差異のみの場合も、結果が正しいことにしておく
            org = org.toLowerCase();
            for (String obj : list) {
                decodeTmp = codec.decode(s, obj);
                encodeTmp = codec.encode(decodeTmp, obj);

                encodeTmp = encodeTmp.toLowerCase();

                if (org.equals(encodeTmp)) {
                    decodeTmp = decodeTmp.replaceAll("[\r|\n|\t]", " ");
                    return new Text(decodeTmp);
                }

                // Windows-31Jの場合は%81@を変換して再実行
                if (obj.equals("windows-31j")) {
                    encodeTmp = encodeTmp.replaceAll("%81%40", "%81@");
                }
                if (org.equals(encodeTmp)) {
                    decodeTmp = decodeTmp.replaceAll("[\r|\n|\t]", " ");
                    return new Text(decodeTmp);
                }
            }

            // 最初から日本語が入っている場合の対応

            // 全てのパターンがマッチしてしまうため、コメントアウト
            //            for (String obj : list) {
            //                encodeTmp = codec.encode(s, obj);
            //                decodeTmp = codec.decode(encodeTmp, obj);
            //
            //                if (s.equals(decodeTmp)) {
            //                    return new Text(s);
            //                }
            //
            //                org = s.toLowerCase();
            //                encodeTmp = encodeTmp.toLowerCase();
            //                if (org.equals(encodeTmp)) {
            //                    return new Text(s);
            //                }
            //            }

            return new Text("<<" + s + ">>");
        } catch (java.io.UnsupportedEncodingException e) {
            // e.printStackTrace();
            return new Text("<<" + s + ">>");
        } catch (org.apache.commons.codec.DecoderException e) {
            // e.printStackTrace();
            return new Text("<<" + s + ">>");
        }
    }
}
