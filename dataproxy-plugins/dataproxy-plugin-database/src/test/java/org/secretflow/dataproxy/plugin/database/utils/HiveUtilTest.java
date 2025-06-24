package org.secretflow.dataproxy.plugin.database.utils;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
        List<String> filed;
        String querySql = HiveUtil.buildQuerySql("table", new ArrayList<String>(List.of(new String[]{"a"})), "");
        assertEquals("select a from table", querySql);
    }

    @Test
    public void testWrapTableName() {
        String wrapedTableName = HiveUtil.wrapTableName("table");
        assertEquals("table", wrapedTableName);
    }

    @Test
    public void testArrowField2JdbcType() {
        assertEquals("INT", HiveUtil.arrowField2JdbcType(new Field("intVal", new FieldType(true, new ArrowType.Int(8, true), null), null)));
    }

}
