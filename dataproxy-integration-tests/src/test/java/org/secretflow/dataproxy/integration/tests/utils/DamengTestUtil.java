package org.secretflow.dataproxy.integration.tests.utils;

import java.io.InputStream;
import java.util.Properties;

public class DamengTestUtil {
    private static final Properties properties = new Properties();

    static {
        try (InputStream is = DamengTestUtil.class.getResourceAsStream("/test-dameng.conf")) {
            properties.load(is);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getDamengDatabase() {
        return properties.getProperty("test.dameng.database");
    }

    public static String getDamengEndpoint() {
        return properties.getProperty("test.dameng.endpoint");
    }

    public static String getUser() {
        return properties.getProperty("test.dameng.user");
    }

    public static String getPassword() {
        return properties.getProperty("test.dameng.password");
    }

}
