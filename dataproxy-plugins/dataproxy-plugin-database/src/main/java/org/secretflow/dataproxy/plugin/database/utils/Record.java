package org.secretflow.dataproxy.plugin.database.utils;

import lombok.Setter;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

@Setter
public class Record {
    private boolean isLast = false;
    private Vector<Object> values;         // 存储列的值，使用 Vector
    private Map<String, Integer> columnNames;
    private Map<Integer, Integer> columnTypes;   // 存储列的数据类型，使用 Vector
    // 构造函数
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

    // 根据列名获取字段的值
    public Object get(String columnName) {
        return values.get(columnNames.get(columnName));
    }

    // 根据列名设置字段的值
    public void set(String columnName, Object value) {
        columnNames.putIfAbsent(columnName, columnNames.size());
        values.add(columnNames.get(columnName), value);
    }

    public void set(int index, Object value) {
        values.set(index, value);
    }

    public int getColumnType(int columnIndex) {
        return columnTypes.get(columnIndex);
    }
    private void setColumnType(int index, int columnType) {
        columnTypes.put(index, columnType);
    }

    private void fromResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            Object columnValue = rs.getObject(i);  // 通过列索引获取值
            this.set(columnName, columnValue);  // 设置值到Record对象中
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

