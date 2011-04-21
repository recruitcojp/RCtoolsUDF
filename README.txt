***************************************************************************

        hive用ユーザ定義関数（UDF）リクルートセレクト「RCtoolsUDF」

                    - Copyright 2011 Recruit -
***************************************************************************

           ダウンロードいただきましてありがとうございます。

---------------------------------------------------------------------------
■はじめに
---------------------------------------------------------------------------

　本ドキュメントは、「RCtoolsUDF」に関する情報を提供します。

---------------------------------------------------------------------------
■ご利用条件
---------------------------------------------------------------------------

　本ツール一式は使用もしくは再配布について、無料でご利用いただけます。

　配布アーカイブに含まれる著作物に対する権利は、Recruitが保有しており、GNU
一般公衆利用許諾契約に基づいて配布しております。再配布・改変等は契約の範囲
内で自由に行うことが出来ます。詳しくは、添付のGNU一般公衆利用許諾契約書を
お読みください。

　なお、本ツールは一般的な利用において動作を確認しておりますが、ご利用の環
境や状況、設定もしくはプログラム上の不具合等により期待と異なる動作をする場
合が考えられます。本ツールの利用に対する効果は無保証であり、あらゆる不利益
や損害等について、当方は一切の責任をいたしかねますので、ご了承いただきます
ようお願い申し上げます。

---------------------------------------------------------------------------
■インストール方法
---------------------------------------------------------------------------

　javaビルド後に作成された「rctools.jar」を、ご使用の環境にコピーして下さい。

---------------------------------------------------------------------------
■使い方
---------------------------------------------------------------------------

-- 独自関数の追加

ADD JAR /usr/local/hive/lib/rctools.jar;
ADD JAR /usr/local/hive/lib/ojdbc6.jar;
ADD JAR /usr/local/hive/lib/commons-dbcp-1.4.jar;
ADD JAR /usr/local/hive/lib/commons-pool-1.5.5.jar;
CREATE TEMPORARY FUNCTION row_sequence AS 'jp.co.recruit.hadoop.hive.contrib.udf.RowSequence';
CREATE TEMPORARY FUNCTION url_decode AS 'jp.co.recruit.hadoop.hive.contrib.udf.UrlDecode';
CREATE TEMPORARY FUNCTION string_exploder AS 'jp.co.recruit.hadoop.hive.contrib.udtf.StringExploder';
CREATE TEMPORARY FUNCTION oracle_loader AS 'jp.co.recruit.hadoop.hive.contrib.udtf.OracleLoader';
CREATE TEMPORARY FUNCTION rank_sequence AS 'jp.co.recruit.hadoop.hive.contrib.udf.RankSequence';
CREATE TEMPORARY FUNCTION hash AS 'jp.co.recruit.hadoop.hive.contrib.udf.Hash';

-- 独自関数の削除

DELETE JAR /usr/local/hive/lib/rctools.jar;
DROP TEMPORARY FUNCTION row_sequence;
DROP TEMPORARY FUNCTION url_decode;
DROP TEMPORARY FUNCTION string_exploder;
DROP TEMPORARY FUNCTION oracle_loader;
DROP TEMPORARY FUNCTION rank_sequence;
DROP TEMPORARY FUNCTION hash;

-- 独自関数の更新

DELETE JAR /usr/local/hive/lib/rctools.jar;
DROP TEMPORARY FUNCTION row_sequence;
DROP TEMPORARY FUNCTION url_decode;
DROP TEMPORARY FUNCTION string_exploder;
DROP TEMPORARY FUNCTION oracle_loader;
DROP TEMPORARY FUNCTION rank_sequence;
DROP TEMPORARY FUNCTION hash;
!sleep 1;
ADD JAR /usr/local/hive/lib/rctools.jar;
CREATE TEMPORARY FUNCTION row_sequence AS 'jp.co.recruit.hadoop.hive.contrib.udf.RowSequence';
CREATE TEMPORARY FUNCTION url_decode AS 'jp.co.recruit.hadoop.hive.contrib.udf.UrlDecode';
CREATE TEMPORARY FUNCTION string_exploder AS 'jp.co.recruit.hadoop.hive.contrib.udtf.StringExploder';
CREATE TEMPORARY FUNCTION oracle_loader AS 'jp.co.recruit.hadoop.hive.contrib.udtf.OracleLoader';
CREATE TEMPORARY FUNCTION rank_sequence AS 'jp.co.recruit.hadoop.hive.contrib.udf.RankSequence';
CREATE TEMPORARY FUNCTION hash AS 'jp.co.recruit.hadoop.hive.contrib.udf.Hash';

-- 関数説明の一覧表示

DESCRIBE FUNCTION row_sequence;
DESCRIBE FUNCTION url_decode;
DESCRIBE FUNCTION string_exploder;
DESCRIBE FUNCTION oracle_loader;
DESCRIBE FUNCTION rank_sequence;
DESCRIBE FUNCTION hash;

-- [func1] row_sequence ...レコードに一意なシーケンス番号を付与する

SET mapred.map.tasks=1;
SELECT row_sequence(), user, item, dt FROM tmp LIMIT 30;

-- [func2] url_decode ...URLエンコードされた文字をデコードする。

SELECT url_decode('%E3%81%AF%E3%81%A9%E3%81%86%E3%83%BC%E3%81%B7') FROM dual;

-- [func3] string_exploder ...1カラムに複数の値が入っているものを展開する

SELECT string_exploder(",", event_list, date_time) AS (exp_event_list, date_time) FROM hogetbl WHERE col01='ああ' LIMIT 30;

-- [func4] oracle_loader ...OracleDBに対して与えられたSQLを実行しレコードを取得する

SET mapred.map.tasks=1;
SELECT oracle_loader('recruit/recruit@localhost:1521/orcl', 'select * from sample', 'ID', 'NAME') AS (ID, NAME) FROM dual;

-- [func5] rank_sequence ...キーごとに一意なシーケンス番号を付与する

SET mapred.map.tasks=1;
SELECT rank_sequence(user), user, item, dt FROM tmp LIMIT 30;

-- [func6] hash ...指定した文字列のハッシュ値を取得する

SELECT hash(user), user, item, dt FROM tmp LIMIT 30;


			- 以上 -
