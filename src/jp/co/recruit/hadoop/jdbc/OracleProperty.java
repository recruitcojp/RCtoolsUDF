/*
 * OracleProperty.java
 *
 * Copyright (C) 2011 RECRUIT Corporation, All Rights Reserved.
 * RECRUIT Corporation CONFIDENTIAL.
 */
package jp.co.recruit.hadoop.jdbc;

import java.text.ParseException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleProperty implements JDBCServerProperty {

    public static final String DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String DEFAULT_CONNECT_PREFIX = "jdbc:oracle:thin:@";
    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_PORT = "1521";
    private static final Pattern PROP_PATTERN = Pattern
        .compile("^([^/]+)/([^@]+)@([^:]+):([0-9]+)/(.+)$");

    private final String url;
    private final String user;
    private final String password;

    /**
     * @param prop サーバ接続情報 ex) "recruit/recruit@localhost:1521/orcl"
     * @throws ParseException
     */
    public OracleProperty(String prop) throws ParseException {
        Matcher m = PROP_PATTERN.matcher(prop);

        if (!m.matches() || m.groupCount() != 5) {
            throw new ParseException("サーバ接続情報が正しくありません。", 0);
        }
        this.user = m.group(1);
        this.password = m.group(2);
        url = makeUrl(m.group(3), m.group(4), m.group(5));
    }

    public OracleProperty(String host, String port, String service, String user, String password) {
        url = makeUrl(host, port, service);
        this.user = user;
        this.password = password;
    }

    public OracleProperty(String host, Integer port, String service) {
        this(host, port != null ? port.toString() : null, service, null, null);
    }

    private String makeUrl(String host, String port, String service) {
        host = host != null ? host : DEFAULT_HOST;
        port = port != null ? port : DEFAULT_PORT;
        service = service != null ? service : "";
        return DEFAULT_CONNECT_PREFIX + host + ":" + port + ":" + service;
    }

    public OracleProperty(Properties prop) {
        this(prop.getProperty("host", DEFAULT_HOST), prop.getProperty("port", "1521"), prop
            .getProperty("service", "orcl"), prop.getProperty("user", null), prop.getProperty(
            "password", null));
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
