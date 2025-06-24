package org.secretflow.dataproxy.plugin.database.utils;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HiveUtilTest {
    @Mock
    private DatabaseConnectConfig hiveConnectConfig;

    @Test
    public void testInitOdps() {
        when(hiveConnectConfig.endpoint()).thenReturn("endpoint");
        when(hiveConnectConfig.database()).thenReturn("database");
        when(hiveConnectConfig.username()).thenReturn("user");
        when(hiveConnectConfig.password()).thenReturn("password");

        Connection hive = HiveUtil.initHive(hiveConnectConfig);
        assertNotNull(hive);
    }

    @Test
    public void testBuildQuerySql() {

    }

}
