package org.secretflow.dataproxy.integration.tests.utils;

import java.io.InputStream;
import java.util.Properties;

public class OracleTestUtil {
    private static final Properties properties = new Properties();

    static {
        try (InputStream is = OracleTestUtil.class.getResourceAsStream("/test-oracle.conf")) {
            properties.load(is);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getOracleDatabase() {
        return properties.getProperty("test.oracle.database");
    }

    public static String getOracleEndpoint() {
        return properties.getProperty("test.oracle.endpoint");
    }

    public static String getUser() {
        return properties.getProperty("test.oracle.user");
    }

    public static String getPassword() {
        return properties.getProperty("test.oracle.password");
    }

}
