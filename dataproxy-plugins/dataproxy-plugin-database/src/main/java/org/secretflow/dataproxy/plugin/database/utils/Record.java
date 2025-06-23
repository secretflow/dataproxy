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

package org.secretflow.dataproxy.plugin.database.utils;

import lombok.Setter;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

@Setter
public class Record {
    // isLast means that this record is the last record
    private boolean isLast = false;
    private Vector<Object> values;
    private Map<String, Integer> columnNames;
    private Map<Integer, Integer> columnTypes;

    public Record() {
        values = new Vector<>();
        columnTypes = new HashMap<>();
        columnNames = new HashMap<>();
    }

    public Record(ResultSet resultSet) throws SQLException {
        values = new Vector<>();
        columnTypes = new HashMap<>();
        columnNames = new HashMap<>();
        this.fromResultSet(resultSet);
    }

    public Object get(String columnName) {
        return values.get(columnNames.get(columnName));
    }

    public void set(String columnName, Object value) {
        columnNames.putIfAbsent(columnName, columnNames.size());
        values.add(columnNames.get(columnName), value);
    }

    private void setColumnType(int index, int columnType) {
        columnTypes.put(index, columnType);
    }

    private void fromResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            Object columnValue = rs.getObject(i);
            this.set(columnName, columnValue);
            this.setColumnType(i, metaData.getColumnType(i));
        }
    }

    public boolean isLastLine() {
        return this.isLast;
    }

    public Map<String, Object> getData() {
        Map<String, Object> temp = new HashMap<>();
        for(Map.Entry<String, Integer> entry : this.columnNames.entrySet()) {
            temp.put(entry.getKey(), this.values.get(entry.getValue()));
        }
        return temp;
    }
}

