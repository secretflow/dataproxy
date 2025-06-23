package org.secretflow.dataproxy.integration.tests.utils;

import java.io.InputStream;
import java.util.Properties;

public class HiveTestUtil {
    private static final Properties properties = new Properties();

    static {
        try (InputStream is = HiveTestUtil.class.getResourceAsStream("/test-hive.conf")) {
            properties.load(is);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getHiveDatabase() {
        return properties.getProperty("test.hive.database");
    }

    public static String getHiveEndpoint() {
        return properties.getProperty("test.hive.endpoint");
    }

    public static String getUser() {
        return properties.getProperty("test.hive.user");
    }

    public static String getPassword() {
        return properties.getProperty("test.hive.password");
    }

}
