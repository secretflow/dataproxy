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

package org.secretflow.dataproxy.plugin.hive.utils;

import org.apache.arrow.vector.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class HiveUtilTest {

    @Test
    public void testBuildQuerySql() {
        String tableName = "table";
        List<String> fields = new ArrayList<>();
        fields.add("col1");
        fields.add("col2");
        fields.add("col3");
        String stmt = HiveUtil.buildQuerySql(tableName, fields, "region=us, date=2025-06-26");
        assertEquals("select col1,col2,col3 from table where region='us' and date='2025-06-26'", stmt);
    }

    @Test
    public void testJDBCType2ArrowType() {
        assertEquals(Types.MinorType.INT.getType(), HiveUtil.jdbcType2ArrowType("int"));
        assertEquals(Types.MinorType.BIGINT.getType(), HiveUtil.jdbcType2ArrowType("bigint"));
        assertEquals(Types.MinorType.FLOAT4.getType(), HiveUtil.jdbcType2ArrowType("float"));
        assertEquals(Types.MinorType.FLOAT8.getType(), HiveUtil.jdbcType2ArrowType("double"));
        assertEquals(Types.MinorType.VARCHAR.getType(), HiveUtil.jdbcType2ArrowType("varchar"));
        assertEquals(Types.MinorType.BIT.getType(), HiveUtil.jdbcType2ArrowType("boolean"));
        assertEquals(Types.MinorType.DATEDAY.getType(), HiveUtil.jdbcType2ArrowType("date"));
        assertEquals(Types.MinorType.TIMESTAMPMILLI.getType(), HiveUtil.jdbcType2ArrowType("timestamp"));
    }

}
