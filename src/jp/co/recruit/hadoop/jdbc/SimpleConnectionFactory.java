/*
 * SimpleConnectionFactory.java
 *
 * Copyright (C) 2011 RECRUIT Corporation, All Rights Reserved.
 * RECRUIT Corporation CONFIDENTIAL.
 */
package jp.co.recruit.hadoop.jdbc;

import java.sql.DriverManager;

import org.apache.commons.pool.BasePoolableObjectFactory;

public class SimpleConnectionFactory extends BasePoolableObjectFactory {
    String url, user, passwd;

    public SimpleConnectionFactory(String url, String user, String passwd) {
        this.url = url;
        this.user = user;
        this.passwd = passwd;
    }

    public Object makeObject() throws Exception {
        return DriverManager.getConnection(url, user, passwd);
    }
}
