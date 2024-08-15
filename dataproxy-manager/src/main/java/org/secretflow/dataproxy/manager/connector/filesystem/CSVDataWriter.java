/*
 * Copyright 2023 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.manager.connector.filesystem;

import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.dataset.format.CSVFormatConfig;
import org.secretflow.dataproxy.manager.DataWriter;

import com.opencsv.CSVParserBuilder;
import com.opencsv.ICSVParser;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * CSV file data writer
 *
 * @author muhong
 * @date 2023-09-11 12:01
 */
public class CSVDataWriter implements DataWriter {

    // 数据写状态
    private FSDataOutputStream outputStream;
    private ICSVParser rowParser;
    private boolean headerWriteFinished = false;

    public CSVDataWriter(FileSystem fileSystem,
                         String uri,
                         CSVFormatConfig formatConfig) {
        // 配置静态 csv 数据源文件格式解析器
        this.rowParser = new CSVParserBuilder()
            .withSeparator(formatConfig.getSeparator().charAt(0))
            .withQuoteChar(formatConfig.getQuoteChar().charAt(0))
            .withEscapeChar(formatConfig.getEscapeChar().charAt(0))
            .build();

        // 获取文件读取流
        try {
            fileSystem.delete(new Path(uri), true);
            this.outputStream = fileSystem.create(new Path(uri));
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.FILE_WRITE_STREAM_CREATE_FAILED, e.getLocalizedMessage(), e);
        }
    }

    @Override
    public void write(VectorSchemaRoot root) throws IOException {

        // 表头写入
        if (!headerWriteFinished) {
            String[] fieldNames = root.getSchema().getFields().stream().map(Field::getName).toArray(String[]::new);
            String headerLine = this.rowParser.parseToLine(fieldNames, false);
            this.outputStream.write(headerLine.getBytes(StandardCharsets.UTF_8));
            this.headerWriteFinished = true;
        }

        // 数据逐行写入
        for (int row = 0; row < root.getRowCount(); row++) {
            String[] values = new String[root.getFieldVectors().size()];
            for (int col = 0; col < root.getFieldVectors().size(); col++) {
                values[col] = serialize(root.getVector(col).getObject(row));
            }

            String rowLine = "\n" + this.rowParser.parseToLine(values, false);
            this.outputStream.write(rowLine.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void flush() throws IOException {
        if (this.outputStream != null) {
            this.outputStream.flush();
        }
    }

    @Override
    public void destroy() throws IOException {

    }

    @Override
    public void close() throws Exception {
        this.flush();

        if (this.outputStream != null) {
            this.outputStream.close();
        }
    }

    /**
     * 数据序列化为字符串
     *
     * @param value 原始数据
     * @return
     */
    private String serialize(Object value) {
        // 文本数据无法区分为空内容还是null，序列化为空内容
        if (value == null) {
            return "";
        }

        // 字节数组单独处理
        if (value instanceof byte[]) {
            return new String((byte[]) value);
        }

        // 其余类型数据直接调用toString方法
        return value.toString();
    }
}
