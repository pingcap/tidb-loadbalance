package com.tidb.jdbc.conf;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HostInfo {

    public static final int NO_PORT = -1;
    private static final String HOST_PORT_SEPARATOR = ":";
    private final String originalUrl;
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final Map<String, String> hostProperties;

    public HostInfo() {
        this(null, (String)null, -1, (String)null, (String)null, (Map)null);
    }

    public HostInfo(String url, String host, int port, String user, String password) {
        this(url, host, port, user, password, (Map)null);
    }

    public HostInfo(String url, String host, int port, String user, String password, Map<String, String> properties) {
        this.hostProperties = new HashMap();
        this.originalUrl = url;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        if (properties != null) {
            this.hostProperties.putAll(properties);
        }

    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public String getHostPortPair() {
        return this.host + ":" + this.port;
    }

    public String getUser() {
        return this.user;
    }

    public String getPassword() {
        return this.password;
    }

    public Map<String, String> getHostProperties() {
        return Collections.unmodifiableMap(this.hostProperties);
    }

    public String getProperty(String key) {
        return (String)this.hostProperties.get(key);
    }

    public String getDatabase() {
        String database = (String)this.hostProperties.get(PropertyKey.DBNAME.getKeyName());
        return StringUtils.isNullOrEmpty(database) ? "" : database;
    }

    public Properties exposeAsProperties() {
        Properties props = new Properties();
        this.hostProperties.entrySet().stream().forEach((e) -> {
            props.setProperty((String)e.getKey(), e.getValue() == null ? "" : (String)e.getValue());
        });
        props.setProperty(PropertyKey.HOST.getKeyName(), this.getHost());
        props.setProperty(PropertyKey.PORT.getKeyName(), String.valueOf(this.getPort()));
        if (this.getUser() != null) {
            props.setProperty(PropertyKey.USER.getKeyName(), this.getUser());
        }

        if (this.getPassword() != null) {
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), this.getPassword());
        }

        return props;
    }

    public String getDatabaseUrl() {
        return this.originalUrl != null ? this.originalUrl : "";
    }

    public boolean equalHostPortPair(com.mysql.cj.conf.HostInfo hi) {
        return (this.getHost() != null && this.getHost().equals(hi.getHost()) || this.getHost() == null && hi.getHost() == null) && this.getPort() == hi.getPort();
    }

    public String toString() {
        StringBuilder asStr = new StringBuilder(super.toString());
        asStr.append(String.format(" :: {host: \"%s\", port: %d, hostProperties: %s}", this.host, this.port, this.hostProperties));
        return asStr.toString();
    }
}
