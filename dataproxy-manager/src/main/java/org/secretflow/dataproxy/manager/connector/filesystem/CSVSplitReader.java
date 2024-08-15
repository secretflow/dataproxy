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
import org.secretflow.dataproxy.manager.SplitReader;

import com.opencsv.CSVParserBuilder;
import com.opencsv.ICSVParser;
import lombok.extern.slf4j.Slf4j;
import okio.Buffer;
import okio.ByteString;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mozilla.universalchardet.Constants;
import org.mozilla.universalchardet.UniversalDetector;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * CSV file data split reader
 *
 * @author muhong
 * @date 2023-09-11 12:02
 */
@Slf4j
public class CSVSplitReader extends ArrowReader implements SplitReader {

    private static final int FILE_READ_BATCH_SIZE = 3 * 1024 * 1024;

    private static final int ARROW_DATA_ROW_SIZE = 10000;


    private final FSDataInputStream inputStream;
    private Charset charset = null;
    private final Buffer buffer;
    private final ICSVParser rowParser;

    private boolean finished = false;

    private final Schema schema;

    /**
     * Sequential mapping of original data fields to output fields
     */
    private final List<Integer> rawIndexList;

    /**
     * Original data header list
     */
    private List<String> headerList;

    public CSVSplitReader(BufferAllocator allocator,
                          FileSystem fileSystem,
                          String uri,
                          Schema schema,
                          CSVFormatConfig formatConfig,
                          List<String> fieldList) {
        super(allocator);
        this.buffer = new Buffer();
        // Build CSV parser
        this.rowParser = new CSVParserBuilder()
            .withSeparator(formatConfig.getSeparator().charAt(0))
            .withQuoteChar(formatConfig.getQuoteChar().charAt(0))
            .withEscapeChar(formatConfig.getEscapeChar().charAt(0))
            .build();

        // Generate file input stream
        try {
            this.inputStream = fileSystem.open(new Path(uri));
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.FILE_READ_STREAM_CREATE_FAILED, e.getMessage(), e);
        }

        // Parse header
        parseHeader();

        // Infer schema
        Map<String, String> rawToArrowFiledNameMap = MapUtils.isNotEmpty(formatConfig.getFieldMap()) ? formatConfig.getFieldMap() : new HashMap<>();
        if (schema == null) {
            schema = new Schema(this.headerList.stream()
                .map(rawName -> Field.nullable(rawToArrowFiledNameMap.getOrDefault(rawName, rawName), new ArrowType.Utf8()))
                .collect(Collectors.toList())
            );
        }

        // Read by specific order
        if (CollectionUtils.isNotEmpty(fieldList)) {
            this.schema = new Schema(fieldList.stream().map(schema::findField).collect(Collectors.toList()));
        } else {
            this.schema = schema;
        }

        // Generate sequential mapping of original data fields to output fields
        Map<String, String> arrowToRawFiledNameMap = rawToArrowFiledNameMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        this.rawIndexList = this.schema.getFields().stream().map(field -> {
            String rawFieldName = arrowToRawFiledNameMap.getOrDefault(field.getName(), field.getName());
            return this.headerList.indexOf(rawFieldName);
        }).collect(Collectors.toList());
    }

    /**
     * Pre allocate memory
     *
     * @param root       Target root
     * @param targetSize Target size
     */
    private static void preAllocate(VectorSchemaRoot root, int targetSize) {
        for (ValueVector vector : root.getFieldVectors()) {
            // Only for fixed-length type data
            if (vector instanceof BaseFixedWidthVector) {
                ((BaseFixedWidthVector) vector).allocateNew(targetSize);
            }
        }
    }

    /**
     * Deserialize data and write it into vector
     *
     * @param vector Vector
     * @param index  Data col index
     * @param value  Serialized data
     */
    private static void addValueInVector(FieldVector vector, int index, String value) {
        if (value == null || StringUtils.isEmpty(value)) {
            vector.setNull(index);
            return;
        }

        try {
            switch (vector.getMinorType()) {
                case TINYINT:
                    ((TinyIntVector) vector).setSafe(index, Integer.parseInt(value));
                    break;
                case SMALLINT:
                    ((SmallIntVector) vector).setSafe(index, Integer.parseInt(value));
                    break;
                case INT:
                    ((IntVector) vector).setSafe(index, Integer.parseInt(value));
                    break;
                case BIGINT:
                    ((BigIntVector) vector).setSafe(index, Long.parseLong(value));
                    break;
                case UINT1:
                    ((UInt1Vector) vector).setSafe(index, Integer.parseInt(value));
                    break;
                case UINT2:
                    ((UInt2Vector) vector).setSafe(index, Integer.parseInt(value));
                    break;
                case UINT4:
                    ((UInt4Vector) vector).setSafe(index, Integer.parseInt(value));
                    break;
                case UINT8:
                    ((UInt8Vector) vector).setSafe(index, Long.parseLong(value));
                    break;
                case FLOAT4:
                    ((Float4Vector) vector).setSafe(index, Float.parseFloat(value));
                    break;
                case FLOAT8:
                    ((Float8Vector) vector).setSafe(index, Double.parseDouble(value));
                    break;
                case BIT:
                    // Compatible with true/false, 0/1
                    if ("true".equalsIgnoreCase(value)) {
                        ((BitVector) vector).setSafe(index, 1);
                    } else if ("false".equalsIgnoreCase(value)) {
                        ((BitVector) vector).setSafe(index, 0);
                    } else {
                        ((BitVector) vector).setSafe(index, Integer.parseInt(value));
                    }
                    break;
                case DATEDAY:
                    ((DateDayVector) vector).setSafe(index, Integer.parseInt(value));
                    break;
                case DATEMILLI:
                    ((DateMilliVector) vector).setSafe(index, 1000L * LocalDateTime.parse(value).getSecond());
                    break;
                case VARCHAR:
                    ((VarCharVector) vector).setSafe(index, value.getBytes(StandardCharsets.UTF_8));
                    break;
                case VARBINARY:
                    ((VarBinaryVector) vector).setSafe(index, value.getBytes(StandardCharsets.UTF_8));
                    break;
            }
        } catch (NumberFormatException e) {
            throw DataproxyException.of(DataproxyErrorCode.DATA_FORMAT_CONVERT_FAILED,
                String.format("%s field data \"%s\" cannot be cast to %s", vector.getName(), value, vector.getMinorType()), e);
        }
    }

    @Override
    public ArrowReader startRead() {
        return this;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        if (finished) {
            return false;
        }

        VectorSchemaRoot root = getVectorSchemaRoot();
        root.clear();
        preAllocate(root, ARROW_DATA_ROW_SIZE);

        int count = 0;
        while (count < ARROW_DATA_ROW_SIZE) {
            String dataLine = readNextLine();
            if (StringUtils.isEmpty(dataLine)) {
                this.finished = true;
                break;
            }

            String[] serializedDataRow = parseLine(dataLine);
            for (int col = 0; col < serializedDataRow.length; col++) {
                FieldVector fieldVector = root.getVector(col);
                addValueInVector(fieldVector, count, serializedDataRow[this.rawIndexList.get(col)]);
            }
            count++;
            root.setRowCount(count);
        }
        return true;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
        try {
            if (this.inputStream != null) {
                this.inputStream.close();
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    protected Schema readSchema() throws IOException {
        return this.schema;
    }

    /**
     * Parse header
     */
    private void parseHeader() {
        // Parse header line
        String headerLine = readNextLine();
        if (StringUtils.isBlank(headerLine)) {
            throw DataproxyException.of(DataproxyErrorCode.HEADER_LINE_NOT_EXIST);
        }

        String[] headerList = parseLine(headerLine);
        if (headerList == null) {
            throw DataproxyException.of(DataproxyErrorCode.HEADER_LINE_PARSE_FAILED);
        }
        this.headerList = Arrays.asList(headerList);
    }

    /**
     * Parse data line
     *
     * @param line Original data
     * @return Split data list
     */
    private String[] parseLine(String line) {
        try {
            return rowParser.parseLine(line);
        } catch (IOException e) {
            throw DataproxyException.of(DataproxyErrorCode.VALUE_LINE_PARSE_FAILED, e);
        }
    }

    /**
     * Read next line
     *
     * @return Next line
     */
    private String readNextLine() {
        // First determine whether there is a line in the buffer
        // If so, return directly, if not, try to download.
        // If new data is downloaded, it is judged again whether there is a line.
        // If there is no new data to download, the remaining data in the buffer is returned.
        boolean continueDownload;
        do {
            continueDownload = !isLineInBuffer() && downloadRangeToBuffer();
        } while (continueDownload);

        try {
            // Try to detect csv encoding during initialization
            boolean isInit = false;
            if (charset == null) {
                isInit = true;
                detectEncoding(buffer);
            }
            // Check if there is a BOM header, if so remove it
            if (isInit) {
                removeBom(buffer);
            }
            // Read data according to the recognized charset
            return readLineOfCharset(buffer);
        } catch (IOException e) {
            throw DataproxyException.of(DataproxyErrorCode.READ_DATA_LINE_FAILED, e);
        }
    }

    // Detect and remove BOM header of CSV
    private void removeBom(Buffer buffer) {
        try {
            if (buffer.size() != 0) {
                ByteString firstLine = buffer.copy().readByteString();
                switch (firstLine.getByte(0) & 0xFF) {
                    case 0xEF:
                        if (firstLine.size() > 2 &&
                            (firstLine.getByte(1) & 0xFF) == 0xBB
                            && (firstLine.getByte(2) & 0xFF) == 0xBF) {
                            buffer.skip(3);
                        }
                        break;
                    case 0xFE:
                        if (firstLine.size() > 3 &&
                            (firstLine.getByte(1) & 0xFF) == 0xFF
                            && (firstLine.getByte(2) & 0xFF) == 0x00
                            && ((firstLine.getByte(3) & 0xFF) == 0x00)) {
                            buffer.skip(4);
                        } else if (firstLine.size() > 1
                            && ((firstLine.getByte(1) & 0xFF) == 0xFF)) {
                            buffer.skip(2);
                        }
                        break;
                    case 0x00:
                        if (firstLine.size() > 3) {
                            if ((firstLine.getByte(1) & 0xFF) == 0x00) {
                                if ((firstLine.getByte(2) & 0xFF) == 0xFE
                                    && (firstLine.getByte(3) & 0xFF) == 0xFF) {
                                    buffer.skip(4);
                                } else if ((firstLine.getByte(2) & 0xFF) == 0xFF
                                    && (firstLine.getByte(3) & 0xFF) == 0xFE) {
                                    buffer.skip(4);
                                }
                            }
                        }
                        break;
                    case 0xFF:
                        if (firstLine.size() > 3 &&
                            (firstLine.getByte(1) & 0xFF) == 0xFE
                            && (firstLine.getByte(2) & 0xFF) == 0x00
                            && ((firstLine.getByte(3) & 0xFF) == 0x00)) {
                            buffer.skip(4);
                        } else if (firstLine.size() > 1
                            && ((firstLine.getByte(1) & 0xFF) == 0xFE)) {
                            buffer.skip(2);
                        }
                        break;
                }
            }
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.BOM_REMOVE_FAILED, e);
        }
    }

    // Detect charset
    private void detectEncoding(Buffer buffer) {
        try {
            UniversalDetector detector = new UniversalDetector(null);
            ByteString firstLine = buffer.copy().readByteString();
            detector.handleData(firstLine.toByteArray(), 0, firstLine.size());
            detector.dataEnd();
            if (detector.getDetectedCharset() != null) {
                if (!Charset.forName(detector.getDetectedCharset()).equals(StandardCharsets.UTF_8)) {
                    // The consensus that is not UTF-8 is GB18030
                    charset = Charset.forName(Constants.CHARSET_GB18030);
                } else {
                    charset = StandardCharsets.UTF_8;
                }
            } else {
                // Use UTF-8 when the detect fails
                charset = StandardCharsets.UTF_8;
            }
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.DETECT_ENCODING_FAILED, e);
        }
    }

    private String readLineOfCharset(Buffer buffer) throws IOException {
        long locOfN = buffer.indexOf(ByteString.of((byte) '\n'));
        if (locOfN != -1L) {
            if (locOfN > 0 && buffer.getByte(locOfN - 1) == (byte) '\r') {
                // \r\n
                String result = buffer.readString(locOfN - 1, charset);
                buffer.skip(2);
                return result;
            } else {
                // \n
                String result = buffer.readString(locOfN, charset);
                buffer.skip(1);
                return result;
            }
        } else if (buffer.size() != 0L) {
            return buffer.readString(charset);
        } else {
            return null;
        }
    }

    private boolean downloadRangeToBuffer() {
        if (inputStream == null) {
            return false;
        }

        try {
            if (inputStream.available() == 0) {
                return false;
            }

            byte[] bytes = new byte[(int) FILE_READ_BATCH_SIZE];
            int length = inputStream.read(bytes);
            buffer.write(bytes, 0, length);
            return true;
        } catch (IOException e) {
            throw DataproxyException.of(DataproxyErrorCode.FILE_BATCH_DOWNLOAD_FAILED, e);
        }
    }

    private boolean isLineInBuffer() {
        return buffer.indexOf((byte) '\n') != -1L;
    }
}
