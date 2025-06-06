/*
 * Copyright 2025 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.secretflow.dataproxy.plugin.odps.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.dataproxy.plugin.odps.config.OdpsConnectConfig;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
 * @author yuexie
 * @date 2025/2/11 15:19
 **/
@ExtendWith(MockitoExtension.class)
public class OdpsUtilTest {

    @Mock
    private OdpsConnectConfig odpsConnectConfig;

    @Mock
    private TypeInfo typeInfo;

    @Mock
    private DecimalTypeInfo decimalTypeInfo;

    @Mock
    private Column column;


    /**
     * Test Scenario: Test the initOdps method
     */
    @Test
    public void testInitOdps() {

        when(odpsConnectConfig.accessKeyId()).thenReturn("accessKeyId");
        when(odpsConnectConfig.accessKeySecret()).thenReturn("accessKeySecret");
        when(odpsConnectConfig.endpoint()).thenReturn("endpoint");
        when(odpsConnectConfig.projectName()).thenReturn("projectName");

        Odps odps = OdpsUtil.initOdps(odpsConnectConfig);
        assertNotNull(odps);
        assertInstanceOf(AliyunAccount.class, odps.getAccount());
        assertEquals("accessKeyId", ((AliyunAccount) odps.getAccount()).getAccessId());
        assertEquals("accessKeySecret", ((AliyunAccount) odps.getAccount()).getAccessKey());
        assertEquals("endpoint", odps.getEndpoint());
        assertEquals("projectName", odps.getDefaultProject());
    }

    /**
     * Test Scenario: Test the getSqlFlag method
     */
    @Test
    public void testGetSqlFlag() {
        Map<String, String> hints = OdpsUtil.getSqlFlag();
        assertEquals("true", hints.get("odps.sql.type.system.odps2"));
    }

    /**
     * Test Scenario: Test the convertOdpsColumnToArrowField method, of type DECIMAL
     */
    @Test
    public void testConvertOdpsColumnToArrowFieldWithDecimal() {
        when(column.getTypeInfo()).thenReturn(decimalTypeInfo);
        when(decimalTypeInfo.getOdpsType()).thenReturn(OdpsType.DECIMAL);
        when(decimalTypeInfo.getPrecision()).thenReturn(10);
        when(decimalTypeInfo.getScale()).thenReturn(2);
        Field field = OdpsUtil.convertOdpsColumnToArrowField(column);

        assertInstanceOf(ArrowType.Decimal.class, field.getType());
    }

    /**
     * Test Scenario: Test the convertOdpsColumnToArrowField method, of type VARCHAR
     */
    @Test
    public void testConvertOdpsColumnToArrowFieldWithVarchar() {
        when(column.getTypeInfo()).thenReturn(typeInfo);
        when(typeInfo.getOdpsType()).thenReturn(OdpsType.VARCHAR);
        Field field = OdpsUtil.convertOdpsColumnToArrowField(column);
        assertEquals(Types.MinorType.VARCHAR.getType(), field.getType());
    }

    /**
     * Test Scenario: Test the convertOdpsColumnToArrowField method of an unsupported type
     */
    @Test
    public void testConvertOdpsColumnToArrowFieldWithUnsupportedType() {
        when(column.getTypeInfo()).thenReturn(typeInfo);
        when(typeInfo.getOdpsType()).thenReturn(OdpsType.UNKNOWN);
        assertThrows(UnsupportedOperationException.class, () -> OdpsUtil.convertOdpsColumnToArrowField(column));
    }
}